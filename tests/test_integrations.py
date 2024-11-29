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

import fcntl
import glob
import itertools
import json
import os
import platform
import pwd
import random
import re
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import traceback
import unittest
from collections import Counter
from unittest.mock import patch

from bzfs import bzfs
from bzfs.bzfs import die_status, getenv_any, getenv_bool
from tests.test_units import TestIncrementalSendSteps, find_match, stop_on_failure_subtest
from tests.zfs_util import (
    bookmark_name,
    bookmarks,
    build,
    create_bookmark,
    create_filesystem,
    create_volume,
    dataset_exists,
    dataset_property,
    datasets,
    destroy,
    destroy_pool,
    destroy_snapshots,
    is_solaris_zfs,
    run_cmd,
    set_sudo_cmd,
    snapshot_name,
    snapshot_property,
    snapshots,
    take_snapshot,
    zfs_list,
    zfs_set,
    zfs_version,
)

src_pool_name = "wb_src"
dst_pool_name = "wb_dest"
pool_size_bytes = 100 * 1024 * 1024
encryption_algo = "aes-256-gcm"
afix = ""
zpool_features = None
creation_prefix = "bzfs_test:"

zfs_encryption_key_fd, zfs_encryption_key = tempfile.mkstemp(prefix="test_bzfs.key_")
os.write(zfs_encryption_key_fd, "mypasswd".encode("utf-8"))
os.close(zfs_encryption_key_fd)
ssh_config_file_fd, ssh_config_file = tempfile.mkstemp(prefix="ssh_config_file_")
os.chmod(ssh_config_file, mode=stat.S_IRWXU)  # chmod u=rwx,go=
os.write(ssh_config_file_fd, "# Empty ssh_config file".encode("utf-8"))
keylocation = f"file://{zfs_encryption_key}"

rng = random.Random(12345)
has_netcat_prog = shutil.which("nc") is not None


ssh_program = getenv_any("test_ssh_program", "ssh")  # also works with "hpnssh"
sudo_cmd = []
if getenv_bool("test_enable_sudo", True) and (os.geteuid() != 0 or platform.system() == "SunOS"):
    sudo_cmd = ["sudo"]
    set_sudo_cmd(["sudo"])


def suite():
    test_mode = getenv_any("test_mode", "")  # Consider toggling this when testing isolated code changes
    is_adhoc_test = test_mode == "adhoc"  # run only a few isolated changes
    is_functional_test = test_mode == "functional"  # run most tests but only in a single local config combination
    suite = unittest.TestSuite()
    if not is_adhoc_test and not is_functional_test:
        suite.addTest(ParametrizedTestCase.parametrize(IncrementalSendStepsTestCase, {"verbose": True}))

    # for ssh_mode in ["pull-push"]:
    # for ssh_mode in ["local", "pull-push"]:
    # for ssh_mode in []:
    for ssh_mode in ["local"]:
        for min_pipe_transfer_size in [1024**2]:
            for affix in [""]:
                # no_privilege_elevation_modes = []
                no_privilege_elevation_modes = [False]
                if os.geteuid() != 0 and not is_adhoc_test and not is_functional_test:
                    no_privilege_elevation_modes.append(True)
                for no_privilege_elevation in no_privilege_elevation_modes:
                    encrypted_datasets = [False]
                    if not is_adhoc_test and not is_functional_test:
                        encrypted_datasets += [True]
                    for encrypted_dataset in encrypted_datasets:
                        params = {
                            "ssh_mode": ssh_mode,
                            "verbose": True,
                            "min_pipe_transfer_size": min_pipe_transfer_size,
                            "affix": affix,
                            "skip_missing_snapshots": "continue",
                            "no_privilege_elevation": no_privilege_elevation,
                            "encrypted_dataset": encrypted_dataset,
                        }
                        if is_adhoc_test:
                            suite.addTest(ParametrizedTestCase.parametrize(AdhocTestCase, params))
                        else:
                            suite.addTest(ParametrizedTestCase.parametrize(LocalTestCase, params))
    if is_adhoc_test:
        return suite

    # ssh_modes = []
    # ssh_modes = ["pull-push"]
    # ssh_modes = ["local", "pull-push", "push", "pull"]
    # ssh_modes = ["local"]
    ssh_modes = ["local", "pull-push"]
    ssh_modes = ["local"] if is_functional_test else ssh_modes
    for ssh_mode in ssh_modes:
        min_pipe_transfer_sizes = [0, 1024**2]
        min_pipe_transfer_sizes = [0] if is_functional_test else min_pipe_transfer_sizes
        for min_pipe_transfer_size in min_pipe_transfer_sizes:
            # for affix in [""]:
            # for affix in ["", ".  -"]:
            for affix in [".  -"]:
                no_privilege_elevation_modes = [False]
                for no_privilege_elevation in no_privilege_elevation_modes:
                    for encrypted_dataset in [False]:
                        params = {
                            "ssh_mode": ssh_mode,
                            "verbose": True,
                            "min_pipe_transfer_size": min_pipe_transfer_size,
                            "affix": affix,
                            "skip_missing_snapshots": "continue",
                            "no_privilege_elevation": no_privilege_elevation,
                            "encrypted_dataset": encrypted_dataset,
                        }
                        suite.addTest(ParametrizedTestCase.parametrize(FullRemoteTestCase, params))

    if os.geteuid() != 0 and not is_functional_test:
        for ssh_mode in ["pull-push", "pull", "push"]:
            for min_pipe_transfer_size in [0]:
                for affix in [""]:
                    for no_privilege_elevation in [True]:
                        # for encrypted_dataset in []:
                        for encrypted_dataset in [False]:
                            params = {
                                "ssh_mode": ssh_mode,
                                "verbose": False,
                                "min_pipe_transfer_size": min_pipe_transfer_size,
                                "affix": affix,
                                "skip_missing_snapshots": "continue",
                                "no_privilege_elevation": no_privilege_elevation,
                                "encrypted_dataset": encrypted_dataset,
                            }
                            suite.addTest(ParametrizedTestCase.parametrize(MinimalRemoteTestCase, params))
    return suite


#############################################################################
class ParametrizedTestCase(unittest.TestCase):

    def __init__(self, methodName="runTest", param=None):
        super().__init__(methodName)
        self.param = param

    @staticmethod
    def parametrize(testcase_klass, param=None):
        testloader = unittest.TestLoader()
        testnames = testloader.getTestCaseNames(testcase_klass)
        suite = unittest.TestSuite()
        for name in testnames:
            suite.addTest(testcase_klass(name, param=param))
        return suite


#############################################################################
class BZFSTestCase(ParametrizedTestCase):

    def setUp(self):
        global src_pool, dst_pool
        global src_root_dataset, dst_root_dataset
        global afix

        for pool in src_pool_name, dst_pool_name:
            if dataset_exists(pool):
                destroy_pool(pool)
            if not dataset_exists(pool):
                tmp = tempfile.NamedTemporaryFile()
                tmp.seek(pool_size_bytes - 1)
                tmp.write(b"0")
                tmp.seek(0)
                run_cmd(sudo_cmd + ["zpool", "create", "-O", "atime=off", pool, tmp.name])

        src_pool = build(src_pool_name)
        dst_pool = build(dst_pool_name)
        afix = self.param.get("affix", "") if self.param is not None else ""
        src_root_dataset = recreate_filesystem(src_pool_name + "/tmp/" + fix("src"))
        dst_root_dataset = recreate_filesystem(dst_pool_name + "/tmp/" + fix("dst"))

        global zpool_features
        if zpool_features is None:
            zpool_features = {}
            detect_zpool_features("src", src_pool_name)
            print(f"zpool bookmarks feature: {are_bookmarks_enabled('src')}", file=sys.stderr)
            props = zpool_features["src"]
            features = "\n".join(
                [f"{k}: {v}" for k, v in sorted(props.items()) if k.startswith("feature@") or k == "delegation"]
            )
            # print(f"test zpool features: {features}", file=sys.stderr)

    # zpool list -o name|grep '^wb_'|xargs -n 1 -r --verbose zpool destroy; rm -fr /tmp/tmp* /run/user/$UID/bzfs/
    def tearDown(self):
        pass  # tear down is deferred to next setup for easier debugging

    def tearDownAndSetup(self):
        self.tearDown()
        self.setUp()

    def setup_basic(self, volume=False):
        compression_props = ["-o", "compression=on"]
        encryption_props = ["-o", f"encryption={encryption_algo}"]
        if is_solaris_zfs():
            encryption_props += ["-o", f"keysource=passphrase,{keylocation}"]
        else:
            encryption_props += ["-o", "keyformat=passphrase", "-o", f"keylocation={keylocation}"]

        dataset_props = encryption_props + compression_props if self.is_encryption_mode() else compression_props
        src_foo = create_filesystem(src_root_dataset, "foo", props=dataset_props)
        src_foo_a = create_volume(src_foo, "a", size="1M") if volume else create_filesystem(src_foo, "a")
        src_foo_b = create_filesystem(src_foo, "b")
        take_snapshot(src_root_dataset, fix("s1"))
        take_snapshot(src_root_dataset, fix("s2"))
        take_snapshot(src_root_dataset, fix("s3"))
        take_snapshot(src_foo, fix("t1"))
        take_snapshot(src_foo, fix("t2"))
        take_snapshot(src_foo, fix("t3"))
        take_snapshot(src_foo_a, fix("u1"))
        take_snapshot(src_foo_a, fix("u2"))
        take_snapshot(src_foo_a, fix("u3"))

    def setup_basic_with_recursive_replication_done(self):
        self.setup_basic()
        self.run_bzfs(src_root_dataset, dst_root_dataset, "--recursive")
        self.assertSnapshots(dst_root_dataset, 3, "s")
        self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")
        self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))  # b/c src has no snapshots

    @staticmethod
    def log_dir_opt():
        return ["--log-dir", os.path.join(bzfs.get_home_directory(), "bzfs-logs-test")]

    def run_bzfs(
        self,
        *args,
        dry_run=None,
        no_create_bookmark=False,
        no_use_bookmark=False,
        skip_on_error="fail",
        retries=0,
        expected_status=0,
        error_injection_triggers=None,
        delete_injection_triggers=None,
        param_injection_triggers=None,
        inject_params=None,
        max_command_line_bytes=None,
        creation_prefix=None,
        max_exceptions_to_summarize=None,
        max_datasets_per_minibatch_on_list_snaps=None,
    ):
        port = getenv_any("test_ssh_port")  # set this if sshd is on non-standard port: export bzfs_test_ssh_port=12345
        args = list(args)
        src_host = ["--ssh-src-host", "127.0.0.1"]
        dst_host = ["--ssh-dst-host", "127.0.0.1"]
        ssh_dflt_port = "2222" if ssh_program == "hpnssh" else "22"  # see https://www.psc.edu/hpn-ssh-home/hpn-readme/
        src_port = ["--ssh-src-port", ssh_dflt_port if port is None else str(port)]
        dst_port = [] if port is None else ["--ssh-dst-port", str(port)]
        src_user = ["--ssh-src-user", os_username()]
        private_key_file = pwd.getpwuid(os.getuid()).pw_dir + "/.ssh/id_rsa"
        src_private_key = ["--ssh-src-private-key", private_key_file, "--ssh-src-private-key", private_key_file]
        src_ssh_config_file = ["--ssh-src-config-file", ssh_config_file]
        dst_ssh_config_file = ["--ssh-dst-config-file", ssh_config_file]
        params = self.param
        if params and params.get("ssh_mode") == "push":
            args = args + dst_host + dst_port
        elif params and params.get("ssh_mode") == "pull":
            args = args + src_host + src_port
        elif params and params.get("ssh_mode") == "pull-push":
            if getenv_bool("enable_IPv6", True) and rng.randint(0, 2) % 3 == 0:
                src_host = ["--ssh-src-host", "::1"]  # IPv6 syntax for 127.0.0.1 loopback address
                dst_host = ["--ssh-dst-host", "::1"]  # IPv6 syntax for 127.0.0.1 loopback address
            args = args + src_host + dst_host + src_port + dst_port
            if params and "min_pipe_transfer_size" in params and int(params["min_pipe_transfer_size"]) == 0:
                args = args + src_user + src_private_key + src_ssh_config_file + dst_ssh_config_file + ["--ssh-cipher="]
            args = args + ["--bwlimit=10000m"]
        elif params and params.get("ssh_mode", "local") != "local":
            raise ValueError("Unknown ssh_mode: " + params["ssh_mode"])

        if params and params.get("ssh_mode", "local") != "local":
            args = args + [
                "--ssh-src-extra-opts",
                "-o StrictHostKeyChecking=no",
                "--ssh-dst-extra-opts",
                "-o StrictHostKeyChecking=no",
            ]
            if ssh_program == "ssh" and has_netcat_prog and (is_solaris_zfs_at_least_11_4_42() or not is_solaris_zfs()):
                r = rng.randint(0, 2)
                if r % 3 == 0:
                    args = args + ["--ssh-src-extra-opt=-oProxyCommand=nc %h %p"]
                elif r % 3 == 1:
                    args = args + ["--ssh-dst-extra-opt=-oProxyCommand=nc %h %p"]

        for arg in args:
            assert "--retries" not in arg
        args += [f"--retries={retries}"]
        args += self.log_dir_opt()

        if params and "skip_missing_snapshots" in params:
            i = find_match(args, lambda arg: arg.startswith("-"))
            i = 0 if i < 0 else i
            args = args[0:i] + ["--skip-missing-snapshots=" + str(params["skip_missing_snapshots"])] + args[i:]

        if self.is_no_privilege_elevation():
            # test ZFS delegation in combination with --no-privilege-elevation flag
            args = args + ["--no-privilege-elevation"]
            src_permissions = "send"
            if not is_solaris_zfs():
                src_permissions += ",bookmark"
            if delete_injection_triggers is not None:
                src_permissions += ",destroy,mount"
            optional_dst_permissions = ",canmount,mountpoint,readonly,compression,encryption,keylocation,recordsize"
            optional_dst_permissions = (
                ",keylocation,compression"
                if not is_solaris_zfs()
                else ",keysource,encryption,salt,compression,checksum"
            )
            dst_permissions = "mount,create,receive,rollback,destroy" + optional_dst_permissions
            cmd = f"sudo zfs allow -u {os_username()} {src_permissions}".split(" ") + [src_pool_name]
            if dataset_exists(src_pool_name):
                run_cmd(cmd)
            cmd = f"sudo zfs allow -u {os_username()} {dst_permissions}".split(" ") + [dst_pool_name]
            if dataset_exists(dst_pool_name):
                run_cmd(cmd)

        if ssh_program != "ssh" and "--ssh-program" not in args and "--ssh-program=" not in args:
            args = args + ["--ssh-program=" + ssh_program]

        if ssh_program == "hpnssh":
            # see https://www.psc.edu/hpn-ssh-home/hpn-readme/
            args = args + ["--ssh-src-extra-opt=-oFallback=no"]
            args = args + ["--ssh-dst-extra-opt=-oFallback=no"]

        if params and params.get("verbose", None):
            args = args + ["--verbose"]

        if params and "min_pipe_transfer_size" in params:
            old_min_pipe_transfer_size = os.environ.get(bzfs.env_var_prefix + "min_pipe_transfer_size")
            os.environ[bzfs.env_var_prefix + "min_pipe_transfer_size"] = str(int(params["min_pipe_transfer_size"]))

        if dry_run:
            args = args + ["--dryrun=recv"]

        if no_create_bookmark:
            args = args + ["--no-create-bookmark"]

        if no_use_bookmark:
            args = args + ["--no-use-bookmark"]

        if skip_on_error:
            args = args + ["--skip-on-error=" + skip_on_error]

        args = args + ["--exclude-envvar-regex=EDITOR"]

        job = bzfs.Job()
        job.is_test_mode = True
        if error_injection_triggers is not None:
            job.error_injection_triggers = error_injection_triggers

        if delete_injection_triggers is not None:
            job.delete_injection_triggers = delete_injection_triggers

        if param_injection_triggers is not None:
            job.param_injection_triggers = param_injection_triggers

        if inject_params is not None:
            job.inject_params = inject_params

        if max_command_line_bytes is not None:
            job.max_command_line_bytes = max_command_line_bytes

        if creation_prefix is not None:
            job.creation_prefix = creation_prefix

        if max_exceptions_to_summarize is not None:
            job.max_exceptions_to_summarize = max_exceptions_to_summarize

        old_max_datasets_per_minibatch_on_list_snaps = os.environ.get(
            bzfs.env_var_prefix + "max_datasets_per_minibatch_on_list_snaps"
        )
        if max_datasets_per_minibatch_on_list_snaps is not None:
            os.environ[bzfs.env_var_prefix + "max_datasets_per_minibatch_on_list_snaps"] = str(
                max_datasets_per_minibatch_on_list_snaps
            )

        returncode = 0
        try:
            job.run_main(bzfs.argument_parser().parse_args(args), args)
        except subprocess.CalledProcessError as e:
            returncode = e.returncode
            if expected_status != returncode:
                traceback.print_exc()
        except SystemExit as e:
            returncode = e.code
            if expected_status != returncode:
                traceback.print_exc()
        finally:
            if self.is_no_privilege_elevation():
                # revoke all ZFS delegation permissions
                cmd = f"sudo zfs unallow -r -u {os_username()}".split(" ") + [src_pool_name]
                if dataset_exists(src_pool_name):
                    run_cmd(cmd)
                cmd = f"sudo zfs unallow -r -u {os_username()}".split(" ") + [dst_pool_name]
                if dataset_exists(dst_pool_name):
                    run_cmd(cmd)

            if params and "min_pipe_transfer_size" in params:
                if old_min_pipe_transfer_size is None:
                    os.environ.pop(bzfs.env_var_prefix + "min_pipe_transfer_size", None)
                else:
                    os.environ[bzfs.env_var_prefix + "min_pipe_transfer_size"] = old_min_pipe_transfer_size

            if max_datasets_per_minibatch_on_list_snaps is not None:
                if old_max_datasets_per_minibatch_on_list_snaps is None:
                    os.environ.pop(bzfs.env_var_prefix + "max_datasets_per_minibatch_on_list_snaps", None)
                else:
                    os.environ[bzfs.env_var_prefix + "max_datasets_per_minibatch_on_list_snaps"] = (
                        old_max_datasets_per_minibatch_on_list_snaps
                    )

        if isinstance(expected_status, list):
            self.assertIn(returncode, expected_status)
        else:
            self.assertEqual(expected_status, returncode)
        return job

    def assertSnapshotNames(self, dataset, expected_names):
        dataset = build(dataset)
        snap_names = natsorted([snapshot_name(snapshot) for snapshot in snapshots(dataset)])
        expected_names = [fix(name) for name in expected_names]
        self.assertListEqual(expected_names, snap_names)

    def assertSnapshots(self, dataset, expected_num_snapshots, snapshot_prefix="", offset=0):
        expected_names = [f"{snapshot_prefix}{i + 1 + offset}" for i in range(0, expected_num_snapshots)]
        self.assertSnapshotNames(dataset, expected_names)

    def assertBookmarkNames(self, dataset, expected_names):
        dataset = build(dataset)
        snap_names = natsorted([bookmark_name(bookmark) for bookmark in bookmarks(dataset)])
        expected_names = [fix(name) for name in expected_names]
        self.assertListEqual(expected_names, snap_names)

    def is_no_privilege_elevation(self):
        return self.param and self.param.get("no_privilege_elevation", False)

    def is_encryption_mode(self):
        return self.param and self.param.get("encrypted_dataset", False)

    @staticmethod
    def properties_with_special_characters():
        return {
            "compression": "off",
            "bzfs:prop0": "/tmp/dir with  spaces and $ dollar sign-" + str(os.getpid()),
            "bzfs:prop1": "/tmp/dir` ~!@#$%^&*()_+-={}[]|;:<>?,./",  # test escaping
            "bzfs:prop2": "/tmp/foo'bar",
            "bzfs:prop3": '/tmp/foo"bar',
            "bzfs:prop4": "/tmp/foo'ba\"rbaz",
            "bzfs:prop5": '/tmp/foo"ba\'r"baz',
            "bzfs:prop6": "/tmp/foo  bar\t\t\nbaz\n\n\n",
            "bzfs:prop7": "/tmp/foo\\bar",
        }

    def generate_recv_resume_token(self, from_snapshot, to_snapshot, dst_dataset):
        snapshot_opts = to_snapshot if not from_snapshot else f"-i {from_snapshot} {to_snapshot}"
        send = f"sudo zfs send --props --raw --compressed -v {snapshot_opts}"
        c = bzfs.inject_dst_pipe_fail_kbytes
        cmd = ["sh", "-c", f"{send} | dd bs=1024 count={c} 2>/dev/null | sudo zfs receive -v -F -u -s {dst_dataset}"]
        try:
            subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"generate_recv_resume_token stdout: {e.stdout}")
            self.assertIn("Partially received snapshot is saved", e.stderr)
        receive_resume_token = dataset_property(dst_dataset, "receive_resume_token")
        self.assertTrue(receive_resume_token)
        self.assertNotEqual("-", receive_resume_token)
        self.assertNotIn(dst_dataset + to_snapshot[to_snapshot.index("@") :], snapshots(dst_dataset))

    def assert_receive_resume_token(self, dataset, exists) -> str:
        receive_resume_token = dataset_property(dataset, "receive_resume_token")
        if exists:
            self.assertTrue(receive_resume_token)
            self.assertNotEqual("-", receive_resume_token)
        else:
            self.assertEqual("-", receive_resume_token)
        return receive_resume_token


#############################################################################
class AdhocTestCase(BZFSTestCase):
    """For testing isolated changes you are currently working on. You can temporarily change the list of tests here.
    The current list is arbitrary and subject to change at any time."""

    def test_zfs_recv_include_regex_with_duplicate_o_and_x_names(self):
        LocalTestCase(param=self.param).test_zfs_recv_include_regex_with_duplicate_o_and_x_names()

    def test_basic_replication_flat_simple(self):
        FullRemoteTestCase(param=self.param).test_basic_replication_flat_simple()

    def test_zfs_set_via_recv_o(self):
        FullRemoteTestCase(param=self.param).test_zfs_set_via_recv_o()


#############################################################################
class IncrementalSendStepsTestCase(BZFSTestCase):

    def test_snapshot_series_excluding_hourlies(self):
        testcase = {None: ["d1", "h1", "d2", "d3", "d4"]}
        expected_results = ["d1", "d2", "d3", "d4"]
        dst_foo = dst_root_dataset + "/foo"

        src_foo = create_filesystem(src_root_dataset, "foo")
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        self.run_bzfs(src_foo, dst_foo, "--include-snapshot-regex", "d.*", "--exclude-snapshot-regex", "h.*")
        self.assertSnapshotNames(dst_foo, expected_results)

        self.tearDownAndSetup()
        src_foo = create_filesystem(src_root_dataset, "foo")
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        src_snapshot = f"{src_foo}@{expected_results[0]}"
        cmd = f"sudo zfs send {src_snapshot} | sudo zfs receive -F -u {dst_foo}"  # full zfs send
        subprocess.run(cmd, text=True, check=True, shell=True)
        self.assertSnapshotNames(dst_foo, [expected_results[0]])
        self.run_bzfs(src_foo, dst_foo, "--include-snapshot-regex", "d.*", "--exclude-snapshot-regex", "h.*")
        self.assertSnapshotNames(dst_foo, expected_results)

        self.tearDownAndSetup()
        src_foo = create_filesystem(src_root_dataset, "foo")
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        src_snapshot = f"{src_foo}@{expected_results[0]}"
        cmd = f"sudo zfs send {src_snapshot} | sudo zfs receive -F -u {dst_foo}"  # full zfs send
        subprocess.run(cmd, text=True, check=True, shell=True)
        self.assertSnapshotNames(dst_foo, [expected_results[0]])
        if are_bookmarks_enabled("src"):
            create_bookmark(src_foo, expected_results[0], expected_results[0])
            destroy(src_snapshot)
            self.run_bzfs(src_foo, dst_foo, "--include-snapshot-regex", "d.*", "--exclude-snapshot-regex", "h.*")
            self.assertSnapshotNames(dst_foo, expected_results)
            src_snapshot2 = f"{src_foo}@{expected_results[-1]}"
            destroy(src_snapshot2)  # no problem because bookmark still exists
            take_snapshot(src_foo, "d99")
            self.run_bzfs(src_foo, dst_foo, "--include-snapshot-regex", "d.*", "--exclude-snapshot-regex", "h.*")
            self.assertSnapshotNames(dst_foo, expected_results + ["d99"])

        self.tearDownAndSetup()
        src_foo = create_filesystem(src_root_dataset, "foo")
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        src_snapshot = f"{src_foo}@{expected_results[1]}"  # Note: [1]
        cmd = f"sudo zfs send {src_snapshot} | sudo zfs receive -F -u {dst_foo}"  # full zfs send
        subprocess.run(cmd, text=True, check=True, shell=True)
        self.assertSnapshotNames(dst_foo, [expected_results[1]])
        self.run_bzfs(
            src_foo,
            dst_foo,
            "--skip-missing-snapshots=continue",
            "--include-snapshot-regex",
            "d.*",
            "--exclude-snapshot-regex",
            "h.*",
        )
        self.assertSnapshotNames(dst_foo, expected_results[1:])

        self.tearDownAndSetup()
        src_foo = create_filesystem(src_root_dataset, "foo")
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        src_snapshot = f"{src_foo}@{expected_results[1]}"  # Note: [1]
        cmd = f"sudo zfs send {src_snapshot} | sudo zfs receive -F -u {dst_foo}"  # full zfs send
        subprocess.run(cmd, text=True, check=True, shell=True)
        self.assertSnapshotNames(dst_foo, [expected_results[1]])
        self.run_bzfs(
            src_foo,
            dst_foo,
            "--force",
            "--skip-missing-snapshots=continue",
            "--exclude-snapshot-regex",
            ".*",
        )
        self.assertSnapshotNames(dst_foo, [])

        self.tearDownAndSetup()
        src_foo = create_filesystem(src_root_dataset, "foo")
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        src_snapshot = f"{src_foo}@{expected_results[1]}"  # Note: [1]
        cmd = f"sudo zfs send {src_snapshot} | sudo zfs receive -F -u {dst_foo}"  # full zfs send
        subprocess.run(cmd, text=True, check=True, shell=True)
        self.assertSnapshotNames(dst_foo, [expected_results[1]])
        self.run_bzfs(
            src_foo,
            dst_foo,
            "--force",
            "--skip-missing-snapshots=continue",
            "--include-snapshot-regex",
            "!.*",
        )
        self.assertSnapshotNames(dst_foo, [])

    def test_snapshot_series_excluding_hourlies_with_permutations(self):
        for testcase in TestIncrementalSendSteps().permute_snapshot_series(5):
            self.tearDownAndSetup()
            src_foo = create_filesystem(src_root_dataset, "foo")
            dst_foo = dst_root_dataset + "/foo"
            for snapshot in testcase[None]:
                take_snapshot(src_foo, snapshot)
            expected_results = testcase["d"]
            # logging.info(f"input   : {','.join(testcase[None])}")
            # logging.info(f"expected: {','.join(expected_results)}")
            for i in range(0, 2):
                with stop_on_failure_subtest(i=i):
                    self.run_bzfs(
                        src_foo,
                        dst_foo,
                        "--skip-missing-snapshots=continue",
                        "--include-snapshot-regex",
                        "d.*",
                        "--exclude-snapshot-regex",
                        "h.*",
                        dry_run=(i == 0),
                    )
                    if i == 0:
                        self.assertFalse(dataset_exists(dst_foo))
                    else:
                        if len(expected_results) > 0:
                            self.assertSnapshotNames(dst_foo, expected_results)
                        else:
                            self.assertFalse(dataset_exists(dst_foo))


#############################################################################
class LocalTestCase(BZFSTestCase):

    def test_basic_replication_flat_simple(self):
        self.setup_basic()
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                if i <= 1:
                    job = self.run_bzfs(src_root_dataset, dst_root_dataset, dry_run=(i == 0))
                else:
                    job = self.run_bzfs(src_root_dataset, dst_root_dataset, "--quiet", dry_run=(i == 0))
                self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
                if i == 0:
                    self.assertSnapshots(dst_root_dataset, 0)
                else:
                    self.assertSnapshots(dst_root_dataset, 3, "s")
                for loc in ["local", "src", "dst"]:
                    if loc != "local":
                        self.assertTrue(job.is_program_available("zfs", loc))
                    self.assertTrue(job.is_program_available("zpool", loc))
                    self.assertTrue(job.is_program_available("ssh", loc))
                    self.assertTrue(job.is_program_available("sh", loc))
                    self.assertTrue(job.is_program_available("sudo", loc))
                    self.assertTrue(job.is_program_available("zstd", loc))
                    self.assertTrue(job.is_program_available("mbuffer", loc))
                    self.assertTrue(job.is_program_available("pv", loc))
                    self.assertTrue(job.is_program_available("ps", loc))

    def test_basic_replication_recursive1_with_volume(self):
        self.test_basic_replication_recursive1(volume=True)

    def test_basic_replication_recursive1(self, volume=False):
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
        self.setup_basic(volume=volume)
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset, dst_root_dataset, "--recursive", dry_run=(i == 0))
                if i == 0:
                    self.assertSnapshots(dst_root_dataset, 0)
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
                else:
                    self.assertSnapshots(dst_root_dataset, 3, "s")
                    self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")
                    self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))  # b/c src has no snapshots

                    compression_prop = dataset_property(dst_root_dataset + "/foo", "compression")
                    self.assertEqual(compression_prop, "on")
                    encryption_prop = dataset_property(dst_root_dataset, "encryption")
                    self.assertEqual(encryption_prop, "off")
                    encryption_prop = dataset_property(dst_root_dataset + "/foo", "encryption")
                    self.assertEqual(encryption_prop, encryption_algo if self.is_encryption_mode() else "off")
                    encryption_prop = dataset_property(dst_root_dataset + "/foo/a", "encryption")
                    self.assertEqual(encryption_prop, encryption_algo if self.is_encryption_mode() else "off")

    def test_basic_replication_flat_pool(self):
        for child in datasets(src_pool) + datasets(dst_pool):
            destroy(child, recursive=True)
        for snapshot in snapshots(src_pool) + snapshots(dst_pool):
            destroy(snapshot)
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_pool, dst_pool, dry_run=(i == 0))
                self.assertSnapshots(dst_pool, 0, "p")  # nothing has changed

        take_snapshot(src_pool, fix("p1"))
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                if (
                    i > 0 and self.is_no_privilege_elevation()
                ):  # maybe related: https://github.com/openzfs/zfs/issues/10461
                    self.skipTest("'cannot unmount '/wb_dest': permission denied' error on zfs receive -F -u wb_dest")
                self.run_bzfs(src_pool, dst_pool, dry_run=(i == 0))
                if i == 0:
                    self.assertSnapshots(dst_pool, 0, "p")  # nothing has changed
                else:
                    self.assertSnapshots(dst_pool, 1, "p")

        for snapshot in snapshots(dst_pool):
            destroy(snapshot)
        take_snapshot(dst_pool, fix("q1"))
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_pool, dst_pool, "--force", "--force-once", dry_run=(i == 0))
                if i == 0:
                    self.assertSnapshots(dst_pool, 1, "q")
                else:
                    self.assertSnapshots(dst_pool, 1, "p")

    def test_basic_replication_missing_pools(self):
        for child in datasets(src_pool) + datasets(dst_pool):
            destroy(child, recursive=True)
        for snapshot in snapshots(src_pool) + snapshots(dst_pool):
            destroy(snapshot)
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_pool, dst_pool, dry_run=(i == 0))
                self.assertSnapshots(dst_pool, 0)  # nothing has changed

        destroy_pool(dst_pool)
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_pool, dst_pool, dry_run=(i == 0), expected_status=die_status)
                self.assertFalse(dataset_exists(dst_pool))

        destroy_pool(src_pool)
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_pool, dst_pool, dry_run=(i == 0), expected_status=die_status)
                self.assertFalse(dataset_exists(src_pool))

    def test_basic_replication_flat_nonzero_snapshots_create_parents(self):
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/a"))
        self.setup_basic()
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset + "/foo/a", dst_root_dataset + "/foo/a", dry_run=(i == 0))
                self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))
                if i == 0:
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo/a"))
                else:
                    self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")

    def test_basic_replication_flat_no_snapshot_dont_create_parents(self):
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))
        self.setup_basic()
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset + "/foo/b", dst_root_dataset + "/foo/b", dry_run=(i == 0))
                self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))

    def test_basic_replication_flat_simple_with_dry_run_no_send(self):
        self.setup_basic()
        self.assertTrue(dataset_exists(dst_root_dataset))
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                dry_run_no_send = ["--dryrun=send"] if i == 0 else []
                self.run_bzfs(src_root_dataset, dst_root_dataset, *dry_run_no_send)
                if i == 0:
                    self.assertSnapshots(dst_root_dataset, 0)
                else:
                    self.assertSnapshots(dst_root_dataset, 3, "s")

    def test_basic_replication_flat_nothing_todo(self):
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset, dst_root_dataset, dry_run=(i == 0))
                self.assertSnapshots(dst_root_dataset, 0)
                self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))

    def test_basic_replication_without_source(self):
        destroy(src_root_dataset, recursive=True)
        recreate_filesystem(dst_root_dataset)
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset, dst_root_dataset, dry_run=(i == 0), expected_status=die_status)
                self.assertTrue(dataset_exists(dst_root_dataset))
                self.assertSnapshots(dst_root_dataset, 0)

    def test_basic_replication_flat_simple_with_skip_parent(self):
        self.setup_basic()
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset, dst_root_dataset, "--skip-parent", dry_run=(i == 0))
                self.assertSnapshots(src_root_dataset, 3, "s")
                self.assertTrue(dataset_exists(dst_root_dataset))
                self.assertSnapshots(dst_root_dataset, 0)

        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset, dst_root_dataset, "--skip-parent", "--delete-dst-datasets", dry_run=(i == 0)
                )
                self.assertSnapshots(src_root_dataset, 3, "s")
                self.assertTrue(dataset_exists(dst_root_dataset))
                self.assertSnapshots(dst_root_dataset, 0)

    def test_basic_replication_recursive_with_skip_parent(self):
        self.setup_basic()
        destroy(dst_root_dataset, recursive=True)
        self.assertFalse(dataset_exists(dst_root_dataset))
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset, dst_root_dataset, "--skip-parent", "--recursive", dry_run=(i == 0))
                self.assertSnapshots(src_root_dataset, 3, "s")
                if i == 0:
                    self.assertFalse(dataset_exists(dst_root_dataset))
                else:
                    self.assertSnapshots(dst_root_dataset, 0)
                    self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")

        destroy(dst_root_dataset, recursive=True)
        self.assertFalse(dataset_exists(dst_root_dataset))
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset,
                    dst_root_dataset,
                    "--skip-parent",
                    "--delete-dst-datasets",
                    "--recursive",
                    "--log-file-prefix=test_",
                    "--log-file-suffix=-daily",
                    dry_run=(i == 0),
                )
                self.assertSnapshots(src_root_dataset, 3, "s")
                if i == 0:
                    self.assertFalse(dataset_exists(dst_root_dataset))
                else:
                    self.assertSnapshots(dst_root_dataset, 0)
                    self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")

    def test_include_snapshot_time_range_full(self):
        self.setup_basic()
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--include-snapshot-times-and-ranks=60secs ago..2999-01-01",
        )
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
        self.assertSnapshots(dst_root_dataset, 3, "s")

    def test_include_snapshot_time_range_empty(self):
        self.setup_basic()
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--include-snapshot-times-and-ranks=2999-01-01..2999-01-01",
        )
        self.assertSnapshots(dst_root_dataset, 0)

    def test_include_snapshot_time_range_full_with_existing_nonempty_dst(self):
        self.setup_basic()
        self.run_bzfs(src_root_dataset, dst_root_dataset)
        destroy(snapshots(dst_root_dataset)[-1])
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--include-snapshot-times-and-ranks=60secs ago..2999-01-01",
        )
        self.assertSnapshots(dst_root_dataset, 3, "s")

    def test_include_snapshot_time_range_full_with_existing_nonempty_dst_no_use_bookmark(self):
        self.setup_basic()
        self.run_bzfs(src_root_dataset, dst_root_dataset)
        destroy(snapshots(dst_root_dataset)[-1])
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--no-use-bookmark",
            "--include-snapshot-times-and-ranks=60secs ago..2999-01-01",
        )
        self.assertSnapshots(dst_root_dataset, 3, "s")

    def test_include_snapshot_rank_range_full(self):
        self.setup_basic()
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--include-snapshot-times-and-ranks",
            "0..0",
            "latest0%..latest100%",
        )
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
        self.assertSnapshots(dst_root_dataset, 3, "s")

    def test_include_snapshot_rank_range_empty(self):
        self.setup_basic()
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--include-snapshot-times-and-ranks",
            "0..0",
            "latest0%..latest0%",
        )
        self.assertSnapshots(dst_root_dataset, 0)

    def run_snapshot_filters(self, filter1, filter2, filter3):
        self.run_bzfs(src_root_dataset, dst_root_dataset, *filter1, *filter2, *filter3, creation_prefix=creation_prefix)

    def test_snapshot_filter_order_matters(self):
        # "creation" zfs property cannot be manually set or modified or easily mocked.
        # Thus, for checks during testing we use the "bzfs_test:creation" property instead of the "creation" property.
        for snap in ["2024-01-01d", "2024-01-02d", "2024-01-03d", "2024-01-04d", "2024-01-05h"]:
            unix_time = bzfs.unixtime_fromisoformat(snap[0 : len("2024-01-01")])
            take_snapshot(src_root_dataset, fix(snap), props=["-o", creation_prefix + f"creation={unix_time}"])

        regex_filter = ["--include-snapshot-regex=.*d.*"]  # include dailies only
        times_filter = ["--include-snapshot-times-and-ranks=2024-01-03..*"]
        ranks_filter = ["--include-snapshot-times-and-ranks", "0..0", "oldest 1"]

        dataset_exists(dst_root_dataset) and destroy(dst_root_dataset, recursive=True)
        self.run_snapshot_filters(regex_filter, times_filter, ranks_filter)
        self.assertSnapshotNames(dst_root_dataset, ["2024-01-03d"])

        dataset_exists(dst_root_dataset) and destroy(dst_root_dataset, recursive=True)
        self.run_snapshot_filters(regex_filter, ranks_filter, times_filter)
        self.assertFalse(dataset_exists(dst_root_dataset))

        dataset_exists(dst_root_dataset) and destroy(dst_root_dataset, recursive=True)
        self.run_snapshot_filters(ranks_filter, regex_filter, times_filter)
        self.assertFalse(dataset_exists(dst_root_dataset))

        dataset_exists(dst_root_dataset) and destroy(dst_root_dataset, recursive=True)
        self.run_snapshot_filters(times_filter, regex_filter, ranks_filter)
        self.assertSnapshotNames(dst_root_dataset, ["2024-01-03d"])

        dataset_exists(dst_root_dataset) and destroy(dst_root_dataset, recursive=True)
        self.run_snapshot_filters(times_filter, ranks_filter, regex_filter)
        self.assertSnapshotNames(dst_root_dataset, ["2024-01-03d"])

    def test_snapshot_filter_regexes_dont_merge_across_groups(self):
        for snap in ["2024-01-01d", "2024-01-02d", "2024-01-03h", "2024-01-04dt"]:
            unix_time = bzfs.unixtime_fromisoformat(snap[0 : len("2024-01-01")])
            take_snapshot(src_root_dataset, fix(snap), props=["-o", creation_prefix + f"creation={unix_time}"])

        regex_filter_daily = ["--include-snapshot-regex=.*d.*"]  # include dailies only
        regex_filter_test = ["--include-snapshot-regex=.*t.*"]
        ranks_filter = ["--include-snapshot-times-and-ranks", "0..0", "oldest 1"]

        dataset_exists(dst_root_dataset) and destroy(dst_root_dataset, recursive=True)
        self.run_snapshot_filters(regex_filter_daily, ranks_filter, regex_filter_test)
        self.assertFalse(dataset_exists(dst_root_dataset))

        dataset_exists(dst_root_dataset) and destroy(dst_root_dataset, recursive=True)
        self.run_snapshot_filters(regex_filter_daily, regex_filter_test, ranks_filter)
        self.assertSnapshotNames(dst_root_dataset, ["2024-01-01d"])

    def test_snapshot_filter_ranks_dont_merge_across_groups(self):
        for snap in ["2024-01-01d", "2024-01-02h", "2024-01-03d", "2024-01-04dt"]:
            unix_time = bzfs.unixtime_fromisoformat(snap[0 : len("2024-01-01")])
            take_snapshot(src_root_dataset, fix(snap), props=["-o", creation_prefix + f"creation={unix_time}"])

        regex_filter_daily = ["--include-snapshot-regex=.*d.*"]  # include dailies only
        ranks_filter1 = ["--include-snapshot-times-and-ranks", "0..0", "oldest 1..oldest100%"]
        ranks_filter2 = ["--include-snapshot-times-and-ranks", "0..0", "oldest 1"]

        dataset_exists(dst_root_dataset) and destroy(dst_root_dataset, recursive=True)
        self.run_snapshot_filters(ranks_filter1, ranks_filter2, regex_filter_daily)
        self.assertFalse(dataset_exists(dst_root_dataset))

        dataset_exists(dst_root_dataset) and destroy(dst_root_dataset, recursive=True)
        self.run_snapshot_filters(ranks_filter1, regex_filter_daily, ranks_filter2)
        self.assertSnapshotNames(dst_root_dataset, ["2024-01-03d"])

    def test_nostream(self):
        self.setup_basic()
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset, dst_root_dataset, "--no-stream", dry_run=(i == 0))
                if i == 0:
                    self.assertSnapshots(dst_root_dataset, 0)
                else:
                    self.assertSnapshotNames(dst_root_dataset, ["s3"])

        take_snapshot(src_root_dataset, fix("s4"))
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset, dst_root_dataset, "--no-stream", dry_run=(i == 0))
                if i == 0:
                    self.assertSnapshotNames(dst_root_dataset, ["s3"])
                else:
                    self.assertSnapshotNames(dst_root_dataset, ["s3", "s4"])

        take_snapshot(src_root_dataset, fix("s5"))
        take_snapshot(src_root_dataset, fix("s6"))
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset, dst_root_dataset, "--no-stream", dry_run=(i == 0))
                if i == 0:
                    self.assertSnapshotNames(dst_root_dataset, ["s3", "s4"])
                else:
                    self.assertSnapshotNames(dst_root_dataset, ["s3", "s4", "s6"])

    def test_basic_replication_with_no_datasets_1(self):
        self.setup_basic()
        self.run_bzfs(expected_status=2)

    @patch("sys.argv", ["bzfs.py"])
    def test_basic_replication_with_no_datasets_2(self):
        with self.assertRaises(SystemExit) as e:
            bzfs.main()
        self.assertEqual(e.exception.code, 2)

    def test_basic_replication_dataset_with_spaces(self):
        d1 = " foo  zoo  "
        src_foo = create_filesystem(src_root_dataset, d1)
        s1 = fix(" s  nap1   ")
        take_snapshot(src_foo, fix(s1))
        d2 = "..::   exit HOME f1.2 echo "
        src_foo_a = create_filesystem(src_foo, d2)
        t1 = fix(d2 + "snap")
        take_snapshot(src_foo_a, fix(t1))
        self.run_bzfs(src_root_dataset, dst_root_dataset, "--recursive")
        self.assertTrue(dataset_exists(dst_root_dataset + "/" + d1))
        self.assertSnapshotNames(dst_root_dataset + "/" + d1, [s1])
        self.assertTrue(dataset_exists(dst_root_dataset + "/" + d1 + "/" + d2))
        self.assertSnapshotNames(dst_root_dataset + "/" + d1 + "/" + d2, [t1])

    def test_basic_replication_flat_simple_with_multiple_root_datasets(self):
        self.setup_basic()
        param_name = bzfs.env_var_prefix + "reuse_ssh_connection"
        old_value = os.environ.get(param_name)
        try:
            os.environ[param_name] = "False"
            for i in range(0, 2):
                with stop_on_failure_subtest(i=i):
                    self.run_bzfs(
                        src_root_dataset,
                        dst_root_dataset,
                        src_root_dataset,
                        dst_root_dataset,
                        "-v",
                        "-v",
                        dry_run=(i == 0),
                    )
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
                    if i == 0:
                        self.assertSnapshots(dst_root_dataset, 0)
                    else:
                        self.assertSnapshots(dst_root_dataset, 3, "s")
        finally:
            if old_value is None:
                os.environ.pop(param_name, None)
            else:
                os.environ[param_name] = old_value

    def test_basic_replication_flat_simple_with_multiple_root_datasets_with_skip_on_error(self):
        self.setup_basic()
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "_nonexistingdataset1",
                    dst_root_dataset,
                    src_root_dataset,
                    dst_root_dataset,
                    src_root_dataset + "_nonexistingdataset2",
                    dst_root_dataset,
                    "--delete-dst-snapshots",
                    "--delete-dst-datasets",
                    dry_run=(i == 0),
                    skip_on_error="dataset",
                    expected_status=die_status,
                    max_exceptions_to_summarize=1000 if i == 0 else 0,
                )
                self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
                if i == 0:
                    self.assertSnapshots(dst_root_dataset, 0)
                else:
                    self.assertSnapshots(dst_root_dataset, 3, "s")

    def test_basic_replication_flat_with_multiple_root_datasets_converted_from_recursive(self, volume=False):
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
        self.setup_basic(volume=volume)
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                src_datasets = zfs_list([src_root_dataset], types=["filesystem", "volume"], max_depth=None)
                dst_datasets = [
                    bzfs.replace_prefix(src_dataset, src_root_dataset, dst_root_dataset) for src_dataset in src_datasets
                ]
                opts = [elem for pair in zip(src_datasets, dst_datasets) for elem in pair]
                self.run_bzfs(*opts, dry_run=(i == 0))
                if i == 0:
                    self.assertSnapshots(dst_root_dataset, 0)
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
                else:
                    self.assertSnapshots(dst_root_dataset, 3, "s")
                    self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")
                    self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))  # b/c src has no snapshots

                    compression_prop = dataset_property(dst_root_dataset + "/foo", "compression")
                    self.assertEqual(compression_prop, "on")
                    encryption_prop = dataset_property(dst_root_dataset, "encryption")
                    self.assertEqual(encryption_prop, "off")
                    encryption_prop = dataset_property(dst_root_dataset + "/foo", "encryption")
                    self.assertEqual(encryption_prop, encryption_algo if self.is_encryption_mode() else "off")
                    encryption_prop = dataset_property(dst_root_dataset + "/foo/a", "encryption")
                    self.assertEqual(encryption_prop, encryption_algo if self.is_encryption_mode() else "off")

    def test_basic_replication_flat_send_recv_flags(self):
        if self.is_no_privilege_elevation():
            self.skipTest("setting properties via zfs receive -o needs extra permissions")
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/a"))
        self.setup_basic()
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                extra_opt = ""
                if is_zpool_feature_enabled_or_active("src", "feature@large_blocks"):
                    extra_opt = " --large-block"
                props = self.properties_with_special_characters()
                opts = [f"{name}={value}" for name, value in props.items()]
                opts = [f"--zfs-recv-program-opt={item}" for opt in opts for item in ("-o", opt)]
                self.run_bzfs(
                    src_root_dataset + "/foo/a",
                    dst_root_dataset + "/foo/a",
                    "-v",
                    "-v",
                    "--zfs-send-program-opts=-v --dryrun" + extra_opt,
                    "--zfs-recv-program-opts=-v -n",
                    "--zfs-recv-program-opt=-u",
                    *opts,
                    dry_run=(i == 0),
                )
                self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))
                if i == 0:
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo/a"))
                else:
                    foo_a = dst_root_dataset + "/foo/a"
                    self.assertSnapshots(foo_a, 3, "u")
                    for name, value in props.items():
                        self.assertEqual(value, dataset_property(foo_a, name))

    def test_basic_replication_recursive_with_exclude_dataset(self):
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
        self.setup_basic()
        goo = create_filesystem(src_root_dataset, "goo")
        take_snapshot(goo, fix("g1"))
        boo = create_filesystem(src_root_dataset, "boo")
        take_snapshot(boo, fix("b1"))
        moo = create_filesystem(src_root_dataset, "moo")
        take_snapshot(moo, fix("m1"))
        zoo = create_filesystem(src_root_dataset, "zoo")
        take_snapshot(zoo, fix("z1"))
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset,
                    dst_root_dataset,
                    "--recursive",
                    "--include-dataset=",
                    "--include-dataset=/" + src_root_dataset,
                    "--exclude-dataset=/" + src_root_dataset + "/foo",
                    "--include-dataset=/" + src_root_dataset + "/foo",
                    "--exclude-dataset=/" + dst_root_dataset + "/goo/",
                    "--include-dataset=/" + dst_root_dataset + "/goo",
                    "--include-dataset=/xxxxxxxxx",
                    "--exclude-dataset=boo/",
                    "--include-dataset=boo",
                    "--force-rollback-to-latest-snapshot",
                    dry_run=(i == 0),
                )
                self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
                self.assertFalse(dataset_exists(dst_root_dataset + "/goo"))
                self.assertFalse(dataset_exists(dst_root_dataset + "/boo"))
                if i == 0:
                    self.assertSnapshots(dst_root_dataset, 0)
                else:
                    self.assertSnapshots(dst_root_dataset, 3, "s")
                    self.assertSnapshots(dst_root_dataset + "/moo", 1, "m")
                    self.assertSnapshots(dst_root_dataset + "/zoo", 1, "z")

    def test_basic_replication_recursive_with_exclude_property(self):
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
        self.setup_basic()
        goo = create_filesystem(src_root_dataset, "goo")
        take_snapshot(goo, fix("g1"))
        goo_child = create_filesystem(src_root_dataset, "goo/child")
        take_snapshot(goo_child, fix("c1"))
        boo = create_filesystem(src_root_dataset, "boo")
        take_snapshot(boo, fix("b1"))
        moo = create_filesystem(src_root_dataset, "moo")
        take_snapshot(moo, fix("m1"))
        zoo = create_filesystem(src_root_dataset, "zoo")
        take_snapshot(zoo, fix("z1"))
        xoo = create_filesystem(src_root_dataset, "xoo")
        take_snapshot(xoo, fix("x1"))
        sync_false = {"synchoid:sync": "false"}
        sync_true = {"synchoid:sync": "true"}
        sync_true_empty = {"synchoid:sync": ""}
        sync_host_match = {"synchoid:sync": f"xxx.example.com,{socket.getfqdn()}"}
        sync_host_mismatch = {"synchoid:sync": "xxx.example.com"}
        zfs_set([src_root_dataset + "/foo"], sync_false)
        zfs_set([src_root_dataset + "/goo"], sync_false)
        zfs_set([src_root_dataset + "/boo"], sync_host_mismatch)
        zfs_set([src_root_dataset + "/moo"], sync_true)
        zfs_set([src_root_dataset + "/zoo"], sync_true_empty)
        zfs_set([src_root_dataset + "/xoo"], sync_host_match)
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset,
                    dst_root_dataset,
                    "--exclude-dataset-property=synchoid:sync",
                    "--recursive",
                    "--force-rollback-to-latest-snapshot",
                    dry_run=(i == 0),
                )
                self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
                self.assertFalse(dataset_exists(dst_root_dataset + "/goo"))
                self.assertFalse(dataset_exists(dst_root_dataset + "/boo"))
                if i == 0:
                    self.assertSnapshots(dst_root_dataset, 0)
                else:
                    self.assertSnapshots(dst_root_dataset, 3, "s")
                    self.assertSnapshots(dst_root_dataset + "/moo", 1, "m")
                    self.assertSnapshots(dst_root_dataset + "/zoo", 1, "z")
                    self.assertSnapshots(dst_root_dataset + "/xoo", 1, "x")

    def test_basic_replication_recursive_with_exclude_property_with_injected_dataset_deletes(self):
        self.setup_basic()
        moo = create_filesystem(src_root_dataset, "moo")
        take_snapshot(moo, fix("m1"))
        sync_true = {"synchoid:sync": "true"}
        zfs_set([src_root_dataset + "/moo"], sync_true)
        destroy(dst_root_dataset, recursive=True)

        # inject deletes for this many times. only after that stop deleting datasets
        counter = Counter(zfs_list_exclude_property=1)
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--exclude-dataset-property=synchoid:sync",
            "--recursive",
            delete_injection_triggers={"before": counter},
        )
        self.assertFalse(dataset_exists(dst_root_dataset))
        self.assertEqual(0, counter["zfs_list_exclude_property"])

    def test_basic_replication_recursive_with_skip_on_error(self):
        for j in range(0, 3):
            self.tearDownAndSetup()
            src_user1 = create_filesystem(src_root_dataset, "user1")
            src_user1_foo = create_filesystem(src_user1, "foo")
            src_user2 = create_filesystem(src_root_dataset, "user2")
            src_user2_bar = create_filesystem(src_user2, "bar")

            dst_user1 = create_filesystem(dst_root_dataset, "user1")
            dst_user1_foo = dst_root_dataset + "/user1/foo"
            dst_user2 = dst_root_dataset + "/user2"
            dst_user2_bar = dst_root_dataset + "/user2/bar"

            take_snapshot(src_user1, fix("u1"))
            take_snapshot(dst_user1, fix("U1"))  # conflict triggers error as there's no common snapshot

            take_snapshot(src_user1_foo, fix("f1"))
            take_snapshot(src_user2, fix("v1"))
            take_snapshot(src_user2_bar, fix("b1"))

            if j == 0:
                # test skip_on_error='tree'
                for i in range(0, 2):
                    with stop_on_failure_subtest(i=i):
                        self.run_bzfs(
                            src_root_dataset,
                            dst_root_dataset,
                            "--skip-parent",
                            "--recursive",
                            dry_run=(i == 0),
                            skip_on_error="tree",
                            expected_status=die_status,
                        )
                        if i == 0:
                            self.assertFalse(dataset_exists(dst_user1_foo))
                            self.assertFalse(dataset_exists(dst_user2))
                            self.assertSnapshots(dst_user1, 1, "U")
                        else:
                            self.assertSnapshots(dst_user1, 1, "U")
                            self.assertFalse(dataset_exists(dst_user1_foo))
                            self.assertSnapshots(dst_user2, 1, "v")
                            self.assertSnapshots(dst_user2_bar, 1, "b")
            elif j == 1:
                # test skip_on_error='dataset'
                self.run_bzfs(
                    src_root_dataset,
                    dst_root_dataset,
                    "--skip-parent",
                    "--recursive",
                    dry_run=(i == 0),
                    skip_on_error="dataset",
                    expected_status=die_status,
                )
                self.assertSnapshots(dst_user1, 1, "U")
                self.assertSnapshots(dst_user1_foo, 1, "f")
                self.assertSnapshots(dst_user2, 1, "v")
                self.assertSnapshots(dst_user2_bar, 1, "b")
            else:
                # skip_on_error = 'dataset' with a non-existing destination dataset
                destroy(dst_user1, recursive=True)

                # inject send failures before this many tries. only after that succeed the operation
                counter = Counter(full_zfs_send=1)

                self.run_bzfs(
                    src_root_dataset,
                    dst_root_dataset,
                    "--skip-parent",
                    "--recursive",
                    skip_on_error="dataset",
                    expected_status=1,
                    error_injection_triggers={"before": counter},
                )
                self.assertEqual(0, counter["full_zfs_send"])
                self.assertFalse(dataset_exists(dst_user1))
                self.assertSnapshots(dst_user2, 1, "v")
                self.assertSnapshots(dst_user2_bar, 1, "b")

    def test_basic_replication_flat_simple_using_main(self):
        self.setup_basic()
        with patch("sys.argv", ["bzfs.py", src_root_dataset, dst_root_dataset] + self.log_dir_opt()):
            bzfs.main()
        self.assertSnapshots(dst_root_dataset, 3, "s")

        with self.assertRaises(SystemExit) as e:
            with patch("sys.argv", ["bzfs.py", "nonexisting_dataset", dst_root_dataset] + self.log_dir_opt()):
                bzfs.main()
            self.assertEqual(e.exception.code, die_status)

    def test_basic_replication_with_overlapping_datasets(self):
        if self.param and self.param.get("ssh_mode", "local") not in ["local", "pull-push"]:
            self.skipTest("Test is only meaningful in local or pull-push mode")
        self.assertTrue(dataset_exists(src_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.setup_basic()
        self.run_bzfs(src_root_dataset, src_root_dataset, expected_status=die_status)
        self.run_bzfs(dst_root_dataset, dst_root_dataset, expected_status=die_status)
        self.run_bzfs(src_root_dataset, src_root_dataset + "/tmp", "--recursive", expected_status=die_status)
        self.run_bzfs(src_root_dataset + "/tmp", src_root_dataset, "--recursive", expected_status=die_status)
        self.run_bzfs(dst_root_dataset, dst_root_dataset + "/tmp", "--recursive", expected_status=die_status)
        self.run_bzfs(dst_root_dataset + "/tmp", dst_root_dataset, "--recursive", expected_status=die_status)

    def test_max_command_line_bytes(self):
        job = self.run_bzfs(src_root_dataset, dst_root_dataset, "--skip-replication")
        self.assertTrue(job.get_max_command_line_bytes("dst", os_name="Linux") > 0)
        self.assertTrue(job.get_max_command_line_bytes("dst", os_name="FreeBSD") > 0)
        self.assertTrue(job.get_max_command_line_bytes("dst", os_name="SunOS") > 0)
        self.assertTrue(job.get_max_command_line_bytes("dst", os_name="Darwin") > 0)
        self.assertTrue(job.get_max_command_line_bytes("dst", os_name="Windows") > 0)
        self.assertTrue(job.get_max_command_line_bytes("dst", os_name="unknown") > 0)

    def test_zfs_set(self):
        if self.is_no_privilege_elevation():
            self.skipTest("setting properties via zfs receive -o needs extra permissions")
        job = self.run_bzfs(src_root_dataset, dst_root_dataset, "--skip-replication")
        props = self.properties_with_special_characters()
        props_list = [f"{name}={value}" for name, value in props.items()]
        job.zfs_set([], job.params.dst, dst_root_dataset)
        job.zfs_set(props_list, job.params.dst, dst_root_dataset)
        for name, value in props.items():
            self.assertEqual(value, dataset_property(dst_root_dataset, name))

    def test_zfs_set_via_recv_o(self):
        if self.is_no_privilege_elevation():
            self.skipTest("setting properties via zfs receive -o needs extra permissions")
        for i in range(0, 5):
            with stop_on_failure_subtest(i=i):
                if i > 0:
                    self.tearDownAndSetup()
                self.setup_basic()
                props = self.properties_with_special_characters()
                zfs_set([src_root_dataset + "/foo"], props)
                disable_pv = [] if i <= 0 else ["--pv-program=" + bzfs.disable_prg]
                disable_mbuffer = [] if i <= 1 else ["--mbuffer-program=" + bzfs.disable_prg]
                disable_zstd = [] if i <= 2 else ["--compression-program=" + bzfs.disable_prg]
                disable_sh = [] if i <= 3 else ["--shell-program=" + bzfs.disable_prg]
                self.run_bzfs(
                    src_root_dataset + "/foo",
                    dst_root_dataset + "/foo",
                    "--zfs-send-program-opts=",
                    "--zfs-recv-o-include-regex",
                    *list(props.keys()),
                    *disable_pv,
                    *disable_mbuffer,
                    *disable_zstd,
                    *disable_sh,
                )
                for name, value in props.items():
                    self.assertEqual(value, dataset_property(dst_root_dataset + "/foo", name))

    def test_zfs_set_via_set_include(self):
        if self.is_no_privilege_elevation():
            self.skipTest("setting properties via zfs receive -o needs extra permissions")
        self.setup_basic()
        props = self.properties_with_special_characters()
        zfs_set([src_root_dataset + "/foo"], props)
        self.run_bzfs(
            src_root_dataset + "/foo",
            dst_root_dataset + "/foo",
            "--zfs-send-program-opts=",
            "--zfs-set-include-regex",
            *list(props.keys()),
        )
        for name, value in props.items():
            self.assertEqual(value, dataset_property(dst_root_dataset + "/foo", name))

    @staticmethod
    def zfs_recv_x_excludes():
        if is_solaris_zfs():
            return ["effectivereadlimit", "effectivewritelimit", "encryption", "keysource"]
        else:
            return []

    def test_zfs_recv_include_regex(self):
        if self.is_no_privilege_elevation():
            self.skipTest("setting properties via zfs receive -o needs extra permissions")
        self.setup_basic()
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))

        included_props = {"include_bzfs:p1": "value1", "include_bzfs:p2": "value2"}
        excluded_props = {"exclude_bzfs:p3": "value3"}
        zfs_set([src_root_dataset + "/foo"], included_props)
        zfs_set([src_root_dataset + "/foo"], excluded_props)
        self.run_bzfs(
            src_root_dataset + "/foo",
            dst_root_dataset + "/foo",
            "--zfs-send-program-opts=",
            "--zfs-recv-o-targets=full",
            "--zfs-recv-o-sources=local,inherited",
            "--zfs-recv-o-include-regex=include_bzfs.*",
            "--zfs-recv-x-targets=full+incremental",
            "--zfs-recv-x-include-regex=.*",
            "--zfs-recv-x-exclude-regex",
            "include_bzfs.*",
            *self.zfs_recv_x_excludes(),
        )
        self.assertSnapshots(dst_root_dataset + "/foo", 3, "t"),
        for name, value in included_props.items():
            self.assertEqual(value, dataset_property(dst_root_dataset + "/foo", name))
        for name, value in excluded_props.items():
            self.assertEqual("-", dataset_property(dst_root_dataset + "/foo", name))

    def test_zfs_recv_include_regex_with_duplicate_o_and_x_names(self):
        if self.is_no_privilege_elevation():
            self.skipTest("setting properties via zfs receive -o needs extra permissions")
        self.setup_basic()
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))

        included_props = {"include_bzfs:p1": "v1", "include_bzfs:p2": "v2", "include_bzfs:p3": "v3"}
        excluded_props = {"exclude_bzfs:p4": "v4", "exclude_bzfs:p5": "v5"}
        zfs_set([src_root_dataset + "/foo"], included_props)
        zfs_set([src_root_dataset + "/foo"], excluded_props)
        self.run_bzfs(
            src_root_dataset + "/foo",
            dst_root_dataset + "/foo",
            "--zfs-send-program-opts=--raw",
            "--zfs-recv-program-opts",
            "-u -o include_bzfs:p1=v1 -x exclude_bzfs:p4",
            "--zfs-recv-o-include-regex=include_bzfs.*",
            "--zfs-recv-x-include-regex=.*",  # will not append include.* as those names already exist in -o options
            "--zfs-recv-x-exclude-regex",
            "xxxxxx",
            *self.zfs_recv_x_excludes(),
        )
        self.assertSnapshots(dst_root_dataset + "/foo", 3, "t"),
        for name, value in included_props.items():
            self.assertEqual(value, dataset_property(dst_root_dataset + "/foo", name))
        for name, value in excluded_props.items():
            self.assertEqual("-", dataset_property(dst_root_dataset + "/foo", name))

    def test_preserve_recordsize(self):
        if self.is_no_privilege_elevation():
            self.skipTest("setting properties via zfs receive -o needs extra permissions")
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.tearDownAndSetup()
                self.setup_basic()
                self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))

                old_recordsize = int(dataset_property(dst_root_dataset, "recordsize"))
                new_recordsize = 8 * 1024
                assert old_recordsize != new_recordsize
                zfs_set([src_root_dataset + "/foo"], {"recordsize": new_recordsize})
                preserve = ["--zfs-recv-o-include-regex", "recordsize", "volblocksize"] if i > 0 else []
                self.run_bzfs(
                    src_root_dataset + "/foo",
                    dst_root_dataset + "/foo",
                    *preserve,
                    "--zfs-send-program-opts=",
                )
                expected = old_recordsize if i == 0 else new_recordsize
                self.assertEqual(str(expected), dataset_property(dst_root_dataset + "/foo", "recordsize"))

    def test_check_zfs_dataset_busy_without_force(self):
        self.setup_basic()
        inject_params = {"is_zfs_dataset_busy": True}
        self.run_bzfs(src_root_dataset, dst_root_dataset, expected_status=die_status, inject_params=inject_params)

    def test_check_zfs_dataset_busy_with_force(self):
        self.setup_basic()
        inject_params = {"is_zfs_dataset_busy": True}
        self.run_bzfs(
            src_root_dataset, dst_root_dataset, "--force", expected_status=die_status, inject_params=inject_params
        )

    def test_periodic_job_locking(self):
        if is_solaris_zfs():
            self.skipTest(
                "On Solaris fcntl.flock() does not work quite as expected. Solaris grants the lock on both file "
                "descriptors simultaneously; probably because they are both in the same process; seems harmless."
            )
        if self.param and self.param.get("ssh_mode", "local") != "local":
            self.skipTest(
                "run_bzfs changes params so this test only passes in local mode (the feature works "
                "correctly in any mode though)"
            )
        self.setup_basic()
        params = bzfs.Params(bzfs.argument_parser().parse_args(args=[src_root_dataset, dst_root_dataset]))
        lock_file = params.lock_file_name()
        with open(lock_file, "w") as lock_file_fd:
            # Acquire an exclusive lock; will raise an error if the lock is already held by another process.
            # The (advisory) lock is auto-released when the process terminates or the file descriptor is closed.
            self.assertTrue(os.path.exists(lock_file))
            fcntl.flock(lock_file_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # LOCK_NB ... non-blocking
            try:
                # job will fail because the lock is already held
                self.run_bzfs(src_root_dataset, dst_root_dataset, expected_status=die_status)
                self.assertTrue(os.path.exists(lock_file))
            finally:
                bzfs.unlink_missing_ok(lock_file)  # avoid accumulation of stale lock files
                self.assertFalse(os.path.exists(lock_file))

    def test_basic_replication_with_delegation_disabled(self):
        if not self.is_no_privilege_elevation():
            self.skipTest("Test requires --no-privilege-elevation")
        self.setup_basic()

        run_cmd(sudo_cmd + ["zpool", "set", "delegation=off", src_pool_name])
        self.run_bzfs(src_root_dataset, dst_root_dataset, expected_status=die_status)

        run_cmd(sudo_cmd + ["zpool", "set", "delegation=on", src_pool_name])
        run_cmd(sudo_cmd + ["zpool", "set", "delegation=off", dst_pool_name])
        self.run_bzfs(src_root_dataset, dst_root_dataset, expected_status=die_status)
        run_cmd(sudo_cmd + ["zpool", "set", "delegation=on", dst_pool_name])

    def test_regex_compilation_error(self):
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--include-snapshot-regex=(xxx",
            "--skip-missing-snapshots=dataset",
            expected_status=die_status,
        )

    def test_basic_replication_skip_missing_snapshots(self):
        self.assertTrue(dataset_exists(src_root_dataset))
        destroy(dst_root_dataset)
        self.run_bzfs(src_root_dataset, dst_root_dataset, "--skip-missing-snapshots=fail", expected_status=die_status)
        self.assertFalse(dataset_exists(dst_root_dataset))
        self.run_bzfs(src_root_dataset, dst_root_dataset, "--skip-missing-snapshots=dataset")
        self.assertFalse(dataset_exists(dst_root_dataset))
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--skip-missing-snapshots=dataset",
            "--include-snapshot-regex=",
            "--recursive",
        )
        self.assertFalse(dataset_exists(dst_root_dataset))
        self.run_bzfs(src_root_dataset, dst_root_dataset, "--skip-missing-snapshots=continue")
        self.assertFalse(dataset_exists(dst_root_dataset))

        self.setup_basic()
        self.assertFalse(dataset_exists(dst_root_dataset))
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--skip-missing-snapshots=fail",
            "--include-snapshot-regex=",
            "--recursive",
            expected_status=die_status,
        )
        self.assertFalse(dataset_exists(dst_root_dataset))
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--skip-missing-snapshots=dataset",
            "--include-snapshot-regex=!.*",
            "--recursive",
        )
        self.assertFalse(dataset_exists(dst_root_dataset))
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--skip-missing-snapshots=continue",
            "--include-snapshot-regex=",
            "--recursive",
        )
        self.assertFalse(dataset_exists(dst_root_dataset))

    def test_basic_replication_with_injected_dataset_deletes(self):
        destroy(dst_root_dataset)
        self.setup_basic()
        self.assertTrue(dataset_exists(src_root_dataset))
        self.assertFalse(dataset_exists(dst_root_dataset))

        # inject deletes for this many times. only after that stop deleting datasets
        counter = Counter(zfs_list_snapshot_src=1)

        self.run_bzfs(src_root_dataset, dst_root_dataset, "-v", "-v", delete_injection_triggers={"before": counter})
        self.assertFalse(dataset_exists(src_root_dataset))
        self.assertFalse(dataset_exists(dst_root_dataset))
        self.assertEqual(0, counter["zfs_list_snapshot_dst"])

    def test_basic_replication_flat_simple_with_sufficiently_many_retries_on_error_injection(self):
        self.basic_replication_flat_simple_with_retries_on_error_injection(retries=6, expected_status=0)

    def test_basic_replication_flat_simple_with_insufficiently_many_retries_on_error_injection(self):
        self.basic_replication_flat_simple_with_retries_on_error_injection(retries=5, expected_status=1)

    def basic_replication_flat_simple_with_retries_on_error_injection(self, retries=0, expected_status=0):
        self.setup_basic()
        create_filesystem(dst_root_dataset)

        # inject failures for this many tries. only after that finally succeed the operation
        counter = Counter(zfs_list_snapshot_dst=2, full_zfs_send=2, incr_zfs_send=2)

        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            retries=retries,
            expected_status=expected_status,
            error_injection_triggers={"before": counter},
        )
        self.assertEqual(0, counter["zfs_list_snapshot_dst"])  # i.e, it took 2-0=2 retries to succeed
        self.assertEqual(0, counter["full_zfs_send"])
        self.assertEqual(0, counter["incr_zfs_send"])
        if expected_status == 0:
            self.assertSnapshots(dst_root_dataset, 3, "s")

    def test_basic_replication_flat_simple_with_retryable_run_tasks_error(self):
        self.setup_basic()

        # inject failures before this many tries. only after that succeed the operation
        counter = Counter(retryable_run_tasks=1)

        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            expected_status=1,
            error_injection_triggers={"before": counter},
        )
        self.assertEqual(0, counter["retryable_run_tasks"])
        self.assertEqual(0, len(snapshots(dst_root_dataset, max_depth=None)))

    def test_basic_replication_recursive_simple_with_force_unmount(self):
        if self.is_encryption_mode():
            self.skipTest("encryption key not loaded")
        self.setup_basic()
        self.run_bzfs(src_root_dataset, dst_root_dataset, "--recursive")
        dst_foo = dst_root_dataset + "/foo"
        dst_foo_a = dst_foo + "/a"
        run_cmd(["sudo", "zfs", "mount", dst_foo])
        # run_cmd(['sudo', 'zfs', 'mount', dst_foo_a])
        take_snapshot(dst_foo, fix("x1"))  # --force will need to rollback that dst snap
        take_snapshot(dst_foo_a, fix("y1"))  # --force will need to rollback that dst snap
        # self.run_bzfs(src_root_dataset, dst_root_dataset, '--force', '--recursive')
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--force-rollback-to-latest-common-snapshot",
            "--recursive",
            "--force-unmount",
        )
        self.assertSnapshots(dst_root_dataset, 3, "s")
        self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")
        self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))  # b/c src has no snapshots

    def test_basic_replication_flat_with_bookmarks1(self):
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        take_snapshot(src_root_dataset, fix("d1"))
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset, dst_root_dataset, dry_run=(i == 0))
                if i == 0:
                    self.assertSnapshotNames(src_root_dataset, ["d1"])
                    self.assertBookmarkNames(src_root_dataset, [])
                else:
                    self.assertSnapshotNames(dst_root_dataset, ["d1"])
                    self.assertBookmarkNames(src_root_dataset, ["d1"])

        # delete snapshot, which will cause no problem as we still have its bookmark
        destroy(snapshots(src_root_dataset)[0])
        self.assertSnapshotNames(src_root_dataset, [])
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset, dst_root_dataset, "--skip-missing-snapshots=fail", dry_run=(i == 0))
                self.assertSnapshotNames(dst_root_dataset, ["d1"])  # nothing has changed
                self.assertBookmarkNames(src_root_dataset, ["d1"])  # nothing has changed

        # take another snapshot and replicate it without problems as we still have the bookmark
        take_snapshot(src_root_dataset, fix("d2"))
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset, dst_root_dataset, "--skip-missing-snapshots=fail", dry_run=(i == 0))
                if i == 0:
                    self.assertSnapshotNames(dst_root_dataset, ["d1"])  # nothing has changed
                    self.assertBookmarkNames(src_root_dataset, ["d1"])  # nothing has changed
                else:
                    self.assertSnapshotNames(dst_root_dataset, ["d1", "d2"])
                    self.assertBookmarkNames(src_root_dataset, ["d1", "d2"])

    def test_basic_replication_flat_with_bookmarks2(self):
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        take_snapshot(src_root_dataset, fix("d1"))
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset, dst_root_dataset, dry_run=(i == 0))
                if i == 0:
                    self.assertSnapshotNames(src_root_dataset, ["d1"])
                    self.assertBookmarkNames(src_root_dataset, [])
                else:
                    self.assertSnapshotNames(dst_root_dataset, ["d1"])
                    self.assertBookmarkNames(src_root_dataset, ["d1"])

        # rename snapshot, which will cause no problem as we still have its bookmark
        cmd = sudo_cmd + ["zfs", "rename", snapshots(src_root_dataset)[0], snapshots(src_root_dataset)[0] + "h"]
        run_cmd(cmd)

        for i in range(0, 2):
            # replicate while excluding the rename snapshot
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset,
                    dst_root_dataset,
                    "--exclude-snapshot-regex=.*h",
                    "--skip-missing-snapshots=fail",
                    dry_run=(i == 0),
                )
                self.assertSnapshotNames(dst_root_dataset, ["d1"])  # nothing has changed
                self.assertBookmarkNames(src_root_dataset, ["d1"])  # nothing has changed

        # take another snapshot and replicate it without problems as we still have the bookmark
        take_snapshot(src_root_dataset, fix("d2"))
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset,
                    dst_root_dataset,
                    "--exclude-snapshot-regex=.*h",
                    "--skip-missing-snapshots=fail",
                    dry_run=(i == 0),
                )
                if i == 0:
                    self.assertSnapshotNames(dst_root_dataset, ["d1"])  # nothing has changed
                    self.assertBookmarkNames(src_root_dataset, ["d1"])  # nothing has changed
                else:
                    self.assertSnapshotNames(dst_root_dataset, ["d1", "d2"])
                    self.assertBookmarkNames(src_root_dataset, ["d1", "d2"])

    def test_basic_replication_flat_with_bookmarks3(self):
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        take_snapshot(src_root_dataset, fix("d1"))
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset, dst_root_dataset, "--no-create-bookmark", dry_run=(i == 0))
                if i == 0:
                    self.assertSnapshotNames(src_root_dataset, ["d1"])
                    self.assertBookmarkNames(src_root_dataset, [])
                else:
                    self.assertSnapshotNames(dst_root_dataset, ["d1"])
                    self.assertBookmarkNames(src_root_dataset, [])
        snapshot_tag = snapshots(src_root_dataset)[0].split("@", 1)[1]
        create_bookmark(src_root_dataset, snapshot_tag, snapshot_tag + "h")
        create_bookmark(src_root_dataset, snapshot_tag, snapshot_tag)
        self.assertBookmarkNames(src_root_dataset, ["d1", "d1h"])

        # delete snapshot, which will cause no problem as we still have its bookmark
        destroy(snapshots(src_root_dataset)[0])
        self.assertSnapshotNames(src_root_dataset, [])

        for i in range(1, 2):
            # replicate while excluding hourly snapshots
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset,
                    dst_root_dataset,
                    "--exclude-snapshot-regex=.*h",
                    "--skip-missing-snapshots=fail",
                    dry_run=(i == 0),
                )
                self.assertSnapshotNames(dst_root_dataset, ["d1"])  # nothing has changed
                self.assertBookmarkNames(src_root_dataset, ["d1", "d1h"])  # nothing has changed

        # take another snapshot and replicate it without problems as we still have the bookmark
        take_snapshot(src_root_dataset, fix("d2"))
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset,
                    dst_root_dataset,
                    "--exclude-snapshot-regex=.*h",
                    "--skip-missing-snapshots=fail",
                    dry_run=(i == 0),
                )
                if i == 0:
                    self.assertSnapshotNames(dst_root_dataset, ["d1"])  # nothing has changed
                    self.assertBookmarkNames(src_root_dataset, ["d1", "d1h"])  # nothing has changed
                else:
                    self.assertSnapshotNames(dst_root_dataset, ["d1", "d2"])
                    self.assertBookmarkNames(src_root_dataset, ["d1", "d1h", "d2"])

    def test_basic_replication_flat_with_bookmarks_already_exists(self):
        """check that run_bzfs works as usual even if the bookmark already exists"""
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        take_snapshot(src_root_dataset, fix("d1"))
        snapshot_tag = snapshots(src_root_dataset)[0].split("@", 1)[1]

        create_bookmark(src_root_dataset, snapshot_tag, snapshot_tag)

        self.assertBookmarkNames(src_root_dataset, ["d1"])
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset, dst_root_dataset, dry_run=(i == 0))
                if i == 0:
                    self.assertSnapshotNames(src_root_dataset, ["d1"])
                    self.assertBookmarkNames(src_root_dataset, ["d1"])
                    self.assertSnapshotNames(dst_root_dataset, [])
                else:
                    self.assertSnapshotNames(src_root_dataset, ["d1"])
                    self.assertBookmarkNames(src_root_dataset, ["d1"])
                    self.assertSnapshotNames(dst_root_dataset, ["d1"])

    def test_complex_replication_flat_with_no_create_bookmark(self):
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
        self.setup_basic()
        src_foo = build(src_root_dataset + "/foo")
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo", dst_root_dataset + "/foo", dry_run=(i == 0), no_create_bookmark=True
                )
                self.assertSnapshots(dst_root_dataset, 0)
                if i == 0:
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
                else:
                    self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo/a"))
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))

        # on src take some snapshots
        take_snapshot(src_foo, fix("t4"))
        take_snapshot(src_foo, fix("t5"))
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo", dst_root_dataset + "/foo", dry_run=(i == 0), no_create_bookmark=True
                )
                self.assertSnapshots(dst_root_dataset, 0)
                if i == 0:
                    self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo/a"))
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))
                else:
                    self.assertSnapshots(dst_root_dataset + "/foo", 5, "t")

        # on src take another snapshot
        take_snapshot(src_foo, fix("t6"))
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo", dst_root_dataset + "/foo", dry_run=(i == 0), no_create_bookmark=True
                )
                self.assertSnapshots(dst_root_dataset, 0)
                if i == 0:
                    self.assertSnapshots(dst_root_dataset + "/foo", 5, "t")
                else:
                    self.assertSnapshots(dst_root_dataset + "/foo", 6, "t")

        # on dst (rather than src) take some snapshots, which is asking for trouble...
        dst_foo = build(dst_root_dataset + "/foo")
        take_snapshot(dst_foo, fix("t7"))
        take_snapshot(dst_foo, fix("t8"))
        # Conflict: Most recent destination snapshot is more recent than most recent common snapshot
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo",
                    dst_root_dataset + "/foo",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                    expected_status=die_status,
                )
                self.assertSnapshots(dst_root_dataset + "/foo", 8, "t")  # nothing has changed on dst

        # resolve conflict via dst rollback to most recent common snapshot
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo",
                    dst_root_dataset + "/foo",
                    "--force-rollback-to-latest-common-snapshot",
                    "--force-once",
                    dry_run=(i == 0),
                )
                self.assertSnapshots(dst_root_dataset, 0)
                if i == 0:
                    self.assertSnapshots(dst_root_dataset + "/foo", 8, "t")  # nothing has changed on dst
                else:
                    self.assertSnapshots(dst_root_dataset + "/foo", 6, "t")

        # on src and dst, take some snapshots, which is asking for trouble again...
        src_guid = snapshot_property(take_snapshot(src_foo, fix("t7")), "guid")
        dst_guid = snapshot_property(take_snapshot(dst_foo, fix("t7")), "guid")
        # names of t7 are the same but GUIDs are different as they are not replicas of each other - t7 is not a common snapshot.
        self.assertNotEqual(src_guid, dst_guid)
        take_snapshot(dst_foo, fix("t8"))
        # Conflict: Most recent destination snapshot is more recent than most recent common snapshot
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo",
                    dst_root_dataset + "/foo",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                    expected_status=die_status,
                )
                self.assertSnapshots(dst_root_dataset + "/foo", 8, "t")  # nothing has changed on dst
                self.assertEqual(
                    dst_guid, snapshot_property(snapshots(build(dst_root_dataset + "/foo"))[6], "guid")
                )  # nothing has changed on dst

        # resolve conflict via dst rollback to most recent common snapshot prior to replicating
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo",
                    dst_root_dataset + "/foo",
                    "--force-rollback-to-latest-common-snapshot",
                    "--force-once",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                )  # resolve conflict via dst rollback
                self.assertSnapshots(dst_root_dataset, 0)
                if i == 0:
                    self.assertSnapshots(dst_root_dataset + "/foo", 8, "t")  # nothing has changed on dst
                    self.assertEqual(
                        dst_guid, snapshot_property(snapshots(build(dst_root_dataset + "/foo"))[6], "guid")
                    )  # nothing has changed on dst
                else:
                    self.assertSnapshots(dst_root_dataset + "/foo", 7, "t")
                    self.assertEqual(
                        src_guid, snapshot_property(snapshots(build(dst_root_dataset + "/foo"))[6], "guid")
                    )  # now they are true replicas

        # on src delete some snapshots that are older than most recent common snapshot, which is normal and won't cause changes to dst
        destroy(snapshots(src_foo)[0])
        destroy(snapshots(src_foo)[2])
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo", dst_root_dataset + "/foo", dry_run=(i == 0), no_create_bookmark=True
                )
                self.assertSnapshots(dst_root_dataset, 0)
                self.assertSnapshots(dst_root_dataset + "/foo", 7, "t")

        # replicate a child dataset
        self.run_bzfs(src_root_dataset + "/foo/a", dst_root_dataset + "/foo/a", no_create_bookmark=True)
        self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")
        self.assertSnapshots(dst_root_dataset, 0)
        self.assertSnapshots(dst_root_dataset + "/foo", 7, "t")

        # on src delete all snapshots so now there is no common snapshot anymore, which is trouble...
        for snap in snapshots(src_foo):
            destroy(snap)
        take_snapshot(src_foo, fix("t9"))
        take_snapshot(src_foo, fix("t10"))
        take_snapshot(src_foo, fix("t11"))
        self.assertSnapshots(src_root_dataset + "/foo", 3, "t", offset=8)
        # Conflict: no common snapshot was found.
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo",
                    dst_root_dataset + "/foo",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                    expected_status=die_status,
                )
                self.assertSnapshots(dst_root_dataset + "/foo", 7, "t")  # nothing has changed on dst
                self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")

        # resolve conflict via deleting all dst snapshots prior to replication
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                if i > 0 and self.is_encryption_mode():
                    # potential workaround?: rerun once with -R --skip-missing added to zfs_send_program_opts
                    self.skipTest(
                        "zfs receive -F cannot be used to destroy an encrypted filesystem - https://github.com/openzfs/zfs/issues/6793"
                    )
                self.run_bzfs(
                    src_root_dataset + "/foo",
                    dst_root_dataset + "/foo",
                    "--force",
                    "--f1",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                )
                self.assertSnapshots(dst_root_dataset, 0)
                self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")
                if i == 0:
                    self.assertSnapshots(dst_root_dataset + "/foo", 7, "t")  # nothing changed
                else:
                    self.assertSnapshots(dst_root_dataset + "/foo", 3, "t", offset=8)

        # no change on src means replication is a noop:
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo", dst_root_dataset + "/foo", dry_run=(i == 0), no_create_bookmark=True
                )
                self.assertSnapshots(dst_root_dataset, 0)
                self.assertSnapshots(dst_root_dataset + "/foo", 3, "t", offset=8)

        # no change on src means replication is a noop:
        for i in range(0, 2):
            # rollback dst to most recent snapshot
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo",
                    dst_root_dataset + "/foo",
                    "--force-rollback-to-latest-common-snapshot",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                )
                self.assertSnapshots(dst_root_dataset, 0)
                self.assertSnapshots(dst_root_dataset + "/foo", 3, "t", offset=8)

    def test_complex_replication_flat_use_bookmarks_with_volume(self):
        self.test_complex_replication_flat_use_bookmarks(volume=True)

    def test_complex_replication_flat_use_bookmarks(self, volume=False):
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
        self.setup_basic()
        src_foo = build(src_root_dataset + "/foo")
        if volume:
            destroy(src_foo, recursive=True)
            src_foo = create_volume(src_foo, size="1M")
            take_snapshot(src_foo, fix("t1"))
            take_snapshot(src_foo, fix("t2"))
            take_snapshot(src_foo, fix("t3"))

        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset + "/foo", dst_root_dataset + "/foo", dry_run=(i == 0))
                self.assertSnapshots(dst_root_dataset, 0)
                if i == 0:
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
                    self.assertBookmarkNames(src_root_dataset + "/foo", [])
                else:
                    self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")
                    self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3"])
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo/a"))
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))

        # on src take some snapshots
        take_snapshot(src_foo, fix("t4"))
        take_snapshot(src_foo, fix("t5"))
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset + "/foo", dst_root_dataset + "/foo", dry_run=(i == 0))
                self.assertSnapshots(dst_root_dataset, 0)
                if i == 0:
                    self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")
                    self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3"])
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo/a"))
                    self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))
                else:
                    self.assertSnapshots(dst_root_dataset + "/foo", 5, "t")
                    self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5"])

        # on src take another snapshot
        take_snapshot(src_foo, fix("t6"))
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset + "/foo", dst_root_dataset + "/foo", dry_run=(i == 0))
                self.assertSnapshots(dst_root_dataset, 0)
                if i == 0:
                    self.assertSnapshots(dst_root_dataset + "/foo", 5, "t")
                    self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5"])
                else:
                    self.assertSnapshots(dst_root_dataset + "/foo", 6, "t")
                    self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5", "t6"])

        # on dst (rather than src) take some snapshots, which is asking for trouble...
        dst_foo = build(dst_root_dataset + "/foo")
        take_snapshot(dst_foo, fix("t7"))
        take_snapshot(dst_foo, fix("t8"))
        # Conflict: Most recent destination snapshot is more recent than most recent common snapshot
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo", dst_root_dataset + "/foo", dry_run=(i == 0), expected_status=die_status
                )
                self.assertSnapshots(dst_root_dataset + "/foo", 8, "t")  # nothing has changed on dst
                self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5", "t6"])

        # resolve conflict via dst rollback to most recent common snapshot
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo",
                    dst_root_dataset + "/foo",
                    "--force-rollback-to-latest-common-snapshot",
                    "--force-once",
                    dry_run=(i == 0),
                )
                self.assertSnapshots(dst_root_dataset, 0)
                if i == 0:
                    self.assertSnapshots(dst_root_dataset + "/foo", 8, "t")  # nothing has changed on dst
                    self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5", "t6"])
                else:
                    self.assertSnapshots(dst_root_dataset + "/foo", 6, "t")
                    self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5", "t6"])

        # on src and dst, take some snapshots, which is asking for trouble again...
        src_guid = snapshot_property(take_snapshot(src_foo, fix("t7")), "guid")
        dst_guid = snapshot_property(take_snapshot(dst_foo, fix("t7")), "guid")
        # names of t7 are the same but GUIDs are different as they are not replicas of each other - t7 is not a common snapshot.
        self.assertNotEqual(src_guid, dst_guid)
        take_snapshot(dst_foo, fix("t8"))
        # Conflict: Most recent destination snapshot is more recent than most recent common snapshot
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo", dst_root_dataset + "/foo", dry_run=(i == 0), expected_status=die_status
                )
                self.assertSnapshots(dst_root_dataset + "/foo", 8, "t")  # nothing has changed on dst
                self.assertEqual(
                    dst_guid, snapshot_property(snapshots(build(dst_root_dataset + "/foo"))[6], "guid")
                )  # nothing has changed on dst
                self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5", "t6"])

        # resolve conflict via dst rollback to most recent common snapshot prior to replicating
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo",
                    dst_root_dataset + "/foo",
                    "--force-rollback-to-latest-common-snapshot",
                    "--force-once",
                    dry_run=(i == 0),
                )  # resolve conflict via dst rollback
                self.assertSnapshots(dst_root_dataset, 0)
                if i == 0:
                    self.assertSnapshots(dst_root_dataset + "/foo", 8, "t")  # nothing has changed on dst
                    self.assertEqual(
                        dst_guid, snapshot_property(snapshots(build(dst_root_dataset + "/foo"))[6], "guid")
                    )  # nothing has changed on dst
                    self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5", "t6"])
                else:
                    self.assertSnapshots(dst_root_dataset + "/foo", 7, "t")
                    self.assertEqual(
                        src_guid, snapshot_property(snapshots(build(dst_root_dataset + "/foo"))[6], "guid")
                    )  # now they are true replicas
                    self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5", "t6", "t7"])

        # on src delete some snapshots that are older than most recent common snapshot, which is normal and won't cause changes to dst
        destroy(snapshots(src_foo)[0])
        destroy(snapshots(src_foo)[2])
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset + "/foo", dst_root_dataset + "/foo", dry_run=(i == 0))
                self.assertSnapshots(dst_root_dataset, 0)
                self.assertSnapshots(dst_root_dataset + "/foo", 7, "t")
                self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5", "t6", "t7"])

        # replicate a child dataset
        if not volume:
            self.run_bzfs(src_root_dataset + "/foo/a", dst_root_dataset + "/foo/a")
            self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")
            self.assertBookmarkNames(src_root_dataset + "/foo/a", ["u1", "u3"])
            self.assertSnapshots(dst_root_dataset + "/foo", 7, "t")

        # on src delete all snapshots so now there is no common snapshot anymore,
        # which isn't actually trouble because we have bookmarks for them...
        for snap in snapshots(src_foo):
            destroy(snap)
        # No Conflict: no common snapshot was found, but we found a (common) bookmark that can be used instead
        # so replication is a noop and won't fail:
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset + "/foo", dst_root_dataset + "/foo", dry_run=(i == 0))
                self.assertSnapshots(dst_root_dataset + "/foo", 7, "t")  # nothing has changed on dst
                self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5", "t6", "t7"])
                if not volume:
                    self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")

        take_snapshot(src_foo, fix("t9"))
        take_snapshot(src_foo, fix("t10"))
        take_snapshot(src_foo, fix("t11"))
        self.assertSnapshots(src_root_dataset + "/foo", 3, "t", offset=8)

        # No Conflict: no common snapshot was found, but we found a (common) bookmark that can be used instead
        # so replication will succeed:
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset + "/foo", dst_root_dataset + "/foo", dry_run=(i == 0))
                if not volume:
                    self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")
                if i == 0:
                    self.assertSnapshots(dst_root_dataset + "/foo", 7, "t")  # nothing has changed on dst
                    self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5", "t6", "t7"])
                else:
                    self.assertSnapshotNames(
                        dst_root_dataset + "/foo", ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t9", "t10", "t11"]
                    )
                    self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5", "t6", "t7", "t11"])

        # no change on src means replication is a noop:
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(src_root_dataset + "/foo", dst_root_dataset + "/foo", dry_run=(i == 0))
                self.assertSnapshotNames(
                    dst_root_dataset + "/foo", ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t9", "t10", "t11"]
                )
                self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5", "t6", "t7", "t11"])

        # on src delete the most recent snapshot and its bookmark, which is trouble as now src has nothing
        # in common anymore with the most recent dst snapshot:
        destroy(natsorted(snapshots(src_foo), key=lambda s: s)[-1])  # destroy t11
        destroy(natsorted(bookmarks(src_foo), key=lambda b: b)[-1])  # destroy t11
        self.assertSnapshotNames(src_root_dataset + "/foo", ["t9", "t10"])
        self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5", "t6", "t7"])
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo", dst_root_dataset + "/foo", dry_run=(i == 0), expected_status=die_status
                )
                self.assertSnapshotNames(
                    dst_root_dataset + "/foo", ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t9", "t10", "t11"]
                )  # nothing has changed
                self.assertBookmarkNames(
                    src_root_dataset + "/foo", ["t1", "t3", "t5", "t6", "t7"]
                )  # nothing has changed

        # resolve conflict via dst rollback to most recent common snapshot prior to replicating
        take_snapshot(src_foo, fix("t12"))
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset + "/foo",
                    dst_root_dataset + "/foo",
                    "--force-rollback-to-latest-common-snapshot",
                    dry_run=(i == 0),
                )
                if not volume:
                    self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")
                if i == 0:
                    self.assertSnapshotNames(
                        dst_root_dataset + "/foo", ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t9", "t10", "t11"]
                    )  # nothing has changed
                    self.assertBookmarkNames(
                        src_root_dataset + "/foo", ["t1", "t3", "t5", "t6", "t7"]
                    )  # nothing has changed
                else:
                    self.assertSnapshotNames(
                        dst_root_dataset + "/foo", ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t9", "t10", "t12"]
                    )  # nothing has changed
                    self.assertBookmarkNames(src_root_dataset + "/foo", ["t1", "t3", "t5", "t6", "t7", "t12"])

    @staticmethod
    def create_resumable_snapshots(lo: int, hi: int, size_in_bytes: int = 1024 * 1024):
        """large enough to be interruptable and resumable. interacts with bzfs.py/inject_dst_pipe_fail"""
        run_cmd(f"sudo zfs mount {src_root_dataset}".split())
        for j in range(lo, hi):
            tmp_file = "/" + src_root_dataset + "/tmpfile"
            run_cmd(f"sudo dd if=/dev/urandom of={tmp_file} bs={size_in_bytes} count=1".split())
            take_snapshot(src_root_dataset, fix(f"s{j}"))
        run_cmd(f"sudo zfs unmount {src_root_dataset}".split())

    def test_all_snapshots_are_fully_replicated_even_though_every_recv_is_interrupted_and_resumed(self):
        if not is_zpool_recv_resume_feature_enabled_or_active():
            self.skipTest("No recv resume zfs feature is available")
        n = 4
        self.create_resumable_snapshots(1, n)
        prev_token = None
        max_iters = 20
        for i in range(0, max_iters):
            self.run_bzfs(
                src_root_dataset,
                dst_root_dataset,
                "-v",
                expected_status=[0, 1],
                inject_params={"inject_dst_pipe_fail": True},  # interrupt every 'zfs receive' prematurely
            )
            if len(snapshots(dst_root_dataset)) == n - 1:
                break
            curr_token = self.assert_receive_resume_token(dst_root_dataset, exists=True)
            self.assertIsNotNone(curr_token)  # assert clear_resumable_recv_state() didn't get called
            self.assertNotEqual(prev_token, curr_token)  # assert send/recv transfers are making progress
            prev_token = curr_token
        print(f"iterations to fully replicate all snapshots: {i}")
        self.assert_receive_resume_token(dst_root_dataset, exists=False)
        self.assertSnapshots(dst_root_dataset, n - 1, "s")
        self.assertLess(i, max_iters - 1)
        self.assertGreater(i, 2)

    def test_send_full_no_resume_recv_with_resume_token_present(self):
        if not is_zpool_recv_resume_feature_enabled_or_active():
            self.skipTest("No recv resume zfs feature is available")
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                if i > 0:
                    self.tearDownAndSetup()
                self.create_resumable_snapshots(1, 4)
                self.generate_recv_resume_token(None, src_root_dataset + "@s1", dst_root_dataset)
                self.assertSnapshots(dst_root_dataset, 0, "s")
                if i == 0:
                    self.run_bzfs(src_root_dataset, dst_root_dataset, "--no-resume-recv", retries=1)
                    self.assertSnapshots(dst_root_dataset, 3, "s")
                else:
                    self.run_bzfs(src_root_dataset, dst_root_dataset, "--no-resume-recv", retries=0, expected_status=1)
                    self.assertSnapshots(dst_root_dataset, 0, "s")
                    self.run_bzfs(src_root_dataset, dst_root_dataset, "--no-resume-recv", retries=0)
                    self.assertSnapshots(dst_root_dataset, 3, "s")

    def test_send_incr_no_resume_recv_with_resume_token_present(self):
        if not is_zpool_recv_resume_feature_enabled_or_active():
            self.skipTest("No recv resume zfs feature is available")
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                if i > 0:
                    self.tearDownAndSetup()
                self.create_resumable_snapshots(1, 2)
                self.run_bzfs(src_root_dataset, dst_root_dataset)
                self.assert_receive_resume_token(dst_root_dataset, exists=False)
                self.assertSnapshots(dst_root_dataset, 1, "s")
                n = 4
                self.create_resumable_snapshots(2, n)
                self.generate_recv_resume_token(src_root_dataset + "@s1", src_root_dataset + "@s3", dst_root_dataset)
                self.assertSnapshots(dst_root_dataset, 1, "s")
                if i == 0:
                    self.run_bzfs(src_root_dataset, dst_root_dataset, "--no-resume-recv", retries=1)
                    self.assertSnapshots(dst_root_dataset, 3, "s")
                else:
                    self.run_bzfs(src_root_dataset, dst_root_dataset, "--no-resume-recv", retries=0, expected_status=1)
                    self.assertSnapshots(dst_root_dataset, 1, "s")
                    self.run_bzfs(src_root_dataset, dst_root_dataset, "--no-resume-recv", retries=0)
                    self.assertSnapshots(dst_root_dataset, 3, "s")

    def test_send_incr_resume_recv(self):
        if not is_zpool_recv_resume_feature_enabled_or_active():
            self.skipTest("No recv resume zfs feature is available")
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                if i > 0:
                    self.tearDownAndSetup()
                self.create_resumable_snapshots(1, 2)
                self.run_bzfs(src_root_dataset, dst_root_dataset)
                self.assert_receive_resume_token(dst_root_dataset, exists=False)
                self.assertSnapshots(dst_root_dataset, 1, "s")
                for bookmark in bookmarks(src_root_dataset):
                    destroy(bookmark)
                n = 4
                self.create_resumable_snapshots(2, n)
                if i == 0:
                    # inject send failures before this many tries. only after that succeed the operation
                    counter = Counter(incr_zfs_send_params=1)
                    self.run_bzfs(
                        src_root_dataset,
                        dst_root_dataset,
                        retries=1,
                        error_injection_triggers={"before": counter},
                        param_injection_triggers={"incr_zfs_send_params": {"inject_dst_pipe_fail": True}},
                    )
                    self.assertEqual(0, counter["incr_zfs_send_params"])
                else:
                    self.run_bzfs(
                        src_root_dataset,
                        dst_root_dataset,
                        "-v",
                        expected_status=1,
                        inject_params={"inject_dst_pipe_fail": True},
                    )
                    self.assert_receive_resume_token(dst_root_dataset, exists=True)
                    self.assertSnapshots(dst_root_dataset, 1, "s")
                    if i == 1:
                        self.run_bzfs(src_root_dataset, dst_root_dataset)
                    else:
                        destroy_snapshots(src_root_dataset, snapshots(src_root_dataset)[1:])
                        self.create_resumable_snapshots(2, n)
                        self.run_bzfs(src_root_dataset, dst_root_dataset, expected_status=255)
                        self.assert_receive_resume_token(dst_root_dataset, exists=False)
                        self.assertSnapshots(dst_root_dataset, 1, "s")
                        self.run_bzfs(src_root_dataset, dst_root_dataset)
                self.assert_receive_resume_token(dst_root_dataset, exists=False)
                self.assertSnapshots(dst_root_dataset, n - 1, "s")

    def test_send_full_resume_recv(self):
        if not is_zpool_recv_resume_feature_enabled_or_active():
            self.skipTest("No recv resume zfs feature is available")
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                if i > 0:
                    self.tearDownAndSetup()
                destroy(dst_root_dataset, recursive=True)
                self.create_resumable_snapshots(1, 2)
                if i == 0:
                    # inject send failures before this many tries. only after that succeed the operation
                    counter = Counter(full_zfs_send_params=1)
                    self.run_bzfs(
                        src_root_dataset,
                        dst_root_dataset,
                        retries=1,
                        error_injection_triggers={"before": counter},
                        param_injection_triggers={"full_zfs_send_params": {"inject_dst_pipe_fail": True}},
                    )
                    self.assertEqual(0, counter["full_zfs_send_params"])
                else:
                    self.run_bzfs(
                        src_root_dataset,
                        dst_root_dataset,
                        "-v",
                        expected_status=1,
                        inject_params={"inject_dst_pipe_fail": True},
                    )
                    self.assert_receive_resume_token(dst_root_dataset, exists=True)
                    self.assertTrue(dataset_exists(dst_root_dataset))
                    self.assertSnapshots(dst_root_dataset, 0, "s")
                    if i == 1:
                        self.run_bzfs(src_root_dataset, dst_root_dataset)
                    elif i == 2:
                        destroy(snapshots(src_root_dataset)[0])
                        self.create_resumable_snapshots(1, 2)
                        self.assertTrue(dataset_exists(dst_root_dataset))
                        self.run_bzfs(src_root_dataset, dst_root_dataset, expected_status=255)
                        # cannot resume send: 'wb_src/tmp/src@s1' is no longer the same snapshot used in the initial send
                        self.assertFalse(dataset_exists(dst_root_dataset))

                        self.run_bzfs(src_root_dataset, dst_root_dataset)
                self.assert_receive_resume_token(dst_root_dataset, exists=False)
                self.assertSnapshots(dst_root_dataset, 1, "s")

    def test_compare_snapshot_lists_with_nonexisting_source(self):
        destroy(src_root_dataset, recursive=True)
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--skip-replication",
            "--compare-snapshot-lists",
            expected_status=die_status,
        )

    def test_compare_snapshot_lists(self):
        def snapshot_list(_job, location=""):
            log_file = _job.params.log_params.log_file
            tsv_file = glob.glob(log_file[0 : log_file.rindex(".log")] + ".cmp/*.tsv")[0]
            with open(tsv_file, "r", encoding="utf-8") as fd:
                return [line.strip() for line in fd if line.startswith(location) and not line.startswith("#")]

        def stats(_job):
            _lines = snapshot_list(_job)
            _n_src = sum(1 for line in _lines if line.startswith("src"))
            _n_dst = sum(1 for line in _lines if line.startswith("dst"))
            _n_all = sum(1 for line in _lines if line.startswith("all"))
            return _n_src, _n_dst, _n_all

        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                param_name = bzfs.env_var_prefix + "max_datasets_per_batch_on_list_snaps"
                old_value = os.environ.get(param_name)
                try:
                    if i > 0:
                        self.tearDownAndSetup()
                        os.environ[param_name] = "0"
                    job = self.run_bzfs(
                        src_root_dataset, dst_root_dataset, "--skip-replication", "--compare-snapshot-lists"
                    )
                    n_src, n_dst, n_all = stats(job)
                    self.assertEqual(0, n_src)
                    self.assertEqual(0, n_dst)
                    self.assertEqual(0, n_all)

                    job = self.run_bzfs(
                        src_root_dataset, dst_root_dataset, "--skip-replication", "--compare-snapshot-lists", "-r"
                    )
                    n_src, n_dst, n_all = stats(job)
                    self.assertEqual(0, n_src)
                    self.assertEqual(0, n_dst)
                    self.assertEqual(0, n_all)

                    self.setup_basic()
                    cmp_choices = []
                    for w in range(0, len(bzfs.cmp_choices_items)):
                        cmp_choices += map(lambda c: "+".join(c), itertools.combinations(bzfs.cmp_choices_items, w + 1))
                    for cmp in cmp_choices:
                        job = self.run_bzfs(
                            src_root_dataset, dst_root_dataset, "--skip-replication", f"--compare-snapshot-lists={cmp}"
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(3 if "src" in cmp else 0, n_src)
                        self.assertEqual(0, n_dst)
                        self.assertEqual(0, n_all)

                    job = self.run_bzfs(src_root_dataset, dst_root_dataset, "--compare-snapshot-lists")
                    n_src, n_dst, n_all = stats(job)
                    self.assertEqual(0, n_src)
                    self.assertEqual(0, n_dst)
                    self.assertEqual(3, n_all)
                    self.assertEqual(3, len(snapshot_list(job, "all")))
                    lines = snapshot_list(job)
                    self.assertEqual(3, len(lines))
                    rel_name = 3
                    root_dataset = 5
                    rel_dataset = 6
                    name = 7
                    creation = 8
                    written = 9
                    self.assertEqual("@s1", lines[0].split("\t")[rel_name])
                    self.assertEqual("@s2", lines[1].split("\t")[rel_name])
                    self.assertEqual("@s3", lines[2].split("\t")[rel_name])
                    self.assertEqual(src_root_dataset, lines[0].split("\t")[root_dataset])
                    self.assertEqual(src_root_dataset, lines[1].split("\t")[root_dataset])
                    self.assertEqual(src_root_dataset, lines[2].split("\t")[root_dataset])
                    self.assertEqual("", lines[0].split("\t")[rel_dataset])
                    self.assertEqual("", lines[1].split("\t")[rel_dataset])
                    self.assertEqual("", lines[2].split("\t")[rel_dataset])
                    self.assertEqual(src_root_dataset + "@s1", lines[0].split("\t")[name])
                    self.assertEqual(src_root_dataset + "@s2", lines[1].split("\t")[name])
                    self.assertEqual(src_root_dataset + "@s3", lines[2].split("\t")[name])
                    self.assertLessEqual(0, int(lines[0].split("\t")[creation]))
                    self.assertLessEqual(0, int(lines[1].split("\t")[creation]))
                    self.assertLessEqual(0, int(lines[2].split("\t")[creation]))
                    if not is_solaris_zfs():
                        self.assertLessEqual(0, int(lines[0].split("\t")[written]))
                        self.assertLessEqual(0, int(lines[1].split("\t")[written]))
                        self.assertLessEqual(0, int(lines[2].split("\t")[written]))
                    else:
                        self.assertLessEqual("-", lines[0].split("\t")[written])
                        self.assertLessEqual("-", lines[1].split("\t")[written])
                        self.assertLessEqual("-", lines[2].split("\t")[written])

                    for cmp in cmp_choices:
                        job = self.run_bzfs(
                            src_root_dataset,
                            dst_root_dataset,
                            f"--compare-snapshot-lists={cmp}",
                            "-r",
                            max_datasets_per_minibatch_on_list_snaps=1,
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(0, n_src)
                        self.assertEqual(0, n_dst)
                        self.assertEqual(3 + 3 + 3 if "all" in cmp else 0, n_all)

                    for threads in range(1, 6):
                        job = self.run_bzfs(
                            src_root_dataset,
                            dst_root_dataset,
                            "--skip-replication",
                            "--compare-snapshot-lists",
                            "-r",
                            "-v",
                            f"--threads={threads}",
                            max_datasets_per_minibatch_on_list_snaps=1,
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(0, n_src)
                        self.assertEqual(0, n_dst)
                        self.assertEqual(3 + 3 + 3, n_all)

                    job = self.run_bzfs(src_root_dataset, dst_root_dataset, "--compare-snapshot-lists", dry_run=True)

                    job = self.run_bzfs(
                        src_root_dataset,
                        dst_root_dataset,
                        "--skip-replication",
                        "--compare-snapshot-lists",
                        "-r",
                        "--include-snapshot-regex=[su]1.*",
                    )
                    n_src, n_dst, n_all = stats(job)
                    self.assertEqual(0, n_src)
                    self.assertEqual(0, n_dst)
                    self.assertEqual(1 + 1, n_all)

                    job = self.run_bzfs(
                        src_root_dataset,
                        dst_root_dataset,
                        "--skip-replication",
                        "--compare-snapshot-lists",
                        "-r",
                        "--include-snapshot-times-and-ranks",
                        "0..0",
                        "latest 1",
                    )
                    n_src, n_dst, n_all = stats(job)
                    self.assertEqual(0, n_src)
                    self.assertEqual(0, n_dst)
                    self.assertEqual(1 + 1 + 1, n_all)
                    lines = snapshot_list(job)
                    self.assertEqual("@s3", lines[0].split("\t")[rel_name])
                    self.assertEqual("/foo@t3", lines[1].split("\t")[rel_name])
                    self.assertEqual("/foo/a@u3", lines[2].split("\t")[rel_name])

                    job = self.run_bzfs(
                        src_root_dataset,
                        dst_root_dataset,
                        "--skip-replication",
                        "--compare-snapshot-lists",
                        "-r",
                        "--include-snapshot-times-and-ranks=0..2999-01-01",
                    )
                    n_src, n_dst, n_all = stats(job)
                    self.assertEqual(0, n_src)
                    self.assertEqual(0, n_dst)
                    self.assertEqual(3 + 3 + 3, n_all)

                    destroy(snapshots(dst_root_dataset)[0])
                    job = self.run_bzfs(
                        src_root_dataset, dst_root_dataset, "--skip-replication", "--compare-snapshot-lists", "-r"
                    )
                    n_src, n_dst, n_all = stats(job)
                    self.assertEqual(1, n_src)
                    self.assertEqual(0, n_dst)
                    self.assertEqual(3 + 3 + 3 - 1, n_all)

                    destroy_snapshots(dst_root_dataset, snapshots(dst_root_dataset))
                    job = self.run_bzfs(
                        src_root_dataset, dst_root_dataset, "--skip-replication", "--compare-snapshot-lists", "-r"
                    )
                    n_src, n_dst, n_all = stats(job)
                    self.assertEqual(3, n_src)
                    self.assertEqual(0, n_dst)
                    self.assertEqual(3 + 3, n_all)

                    if not are_bookmarks_enabled("src"):
                        continue

                    src_foo = src_root_dataset + "/foo"
                    destroy(snapshots(src_foo)[0])  # but retain bookmark
                    job = self.run_bzfs(
                        src_root_dataset, dst_root_dataset, "--skip-replication", "--compare-snapshot-lists", "-r"
                    )
                    n_src, n_dst, n_all = stats(job)
                    self.assertEqual(3, n_src)
                    self.assertEqual(0, n_dst)
                    self.assertEqual(3 + 3, n_all)
                    lines = snapshot_list(job, "dst")
                    self.assertEqual(0, len(lines))

                    src_foo = src_root_dataset + "/foo"
                    destroy(bookmarks(src_foo)[0])  # also delete bookmark now
                    job = self.run_bzfs(
                        src_root_dataset, dst_root_dataset, "--skip-replication", "--compare-snapshot-lists", "-r"
                    )
                    n_src, n_dst, n_all = stats(job)
                    self.assertEqual(3, n_src)
                    self.assertEqual(1, n_dst)
                    self.assertEqual(3 + 3 - 1, n_all)
                    lines = snapshot_list(job, "dst")
                    self.assertEqual(1, len(lines))
                    self.assertEqual("/foo@t1", lines[0].split("\t")[rel_name])
                    self.assertEqual(dst_root_dataset, lines[0].split("\t")[root_dataset])
                    self.assertEqual(dst_root_dataset + "/foo@t1", lines[0].split("\t")[name])

                    destroy_snapshots(src_foo, snapshots(src_foo))
                    job = self.run_bzfs(
                        src_root_dataset, dst_root_dataset, "--skip-replication", "--compare-snapshot-lists", "-r"
                    )
                    n_src, n_dst, n_all = stats(job)
                    self.assertEqual(3, n_src)
                    self.assertEqual(2, n_dst)
                    self.assertEqual(4, n_all)

                    for bookmark in bookmarks(src_foo):
                        destroy(bookmark)
                    job = self.run_bzfs(
                        src_root_dataset, dst_root_dataset, "--skip-replication", "--compare-snapshot-lists", "-r"
                    )
                    n_src, n_dst, n_all = stats(job)
                    self.assertEqual(3, n_src)
                    self.assertEqual(3, n_dst)
                    self.assertEqual(3, n_all)

                    src_foo_a = src_root_dataset + "/foo/a"
                    dst_foo_a = dst_root_dataset + "/foo/a"
                    destroy(snapshots(src_foo_a)[-1])
                    destroy(snapshots(dst_foo_a)[-1])
                    job = self.run_bzfs(
                        src_root_dataset, dst_root_dataset, "--skip-replication", "--compare-snapshot-lists", "-r"
                    )
                    n_src, n_dst, n_all = stats(job)
                    self.assertEqual(3, n_src)
                    self.assertEqual(3, n_dst)
                    self.assertEqual(3 - 1, n_all)
                finally:
                    if old_value is None:
                        os.environ.pop(param_name, None)
                    else:
                        os.environ[param_name] = old_value

    def test_delete_dst_datasets_with_missing_src_root(self):
        destroy(src_root_dataset, recursive=True)
        recreate_filesystem(dst_root_dataset)
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    src_root_dataset,
                    dst_root_dataset,
                    "--skip-replication",
                    "--delete-dst-datasets",
                    dry_run=(i == 0),
                )
                if i == 0:
                    self.assertTrue(dataset_exists(dst_root_dataset))
                else:
                    self.assertFalse(dataset_exists(dst_root_dataset))

    def test_delete_dst_datasets_recursive_with_dummy_src(self):
        self.setup_basic_with_recursive_replication_done()
        for i in range(0, 3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    bzfs.dummy_dataset,
                    dst_root_dataset,
                    "--skip-replication",
                    "--delete-dst-datasets",
                    dry_run=(i == 0),
                )
                if i == 0:
                    self.assertTrue(dataset_exists(dst_root_dataset))
                else:
                    self.assertFalse(dataset_exists(dst_root_dataset))

    def test_delete_dst_datasets_flat_nothing_todo(self):
        self.setup_basic_with_recursive_replication_done()
        take_snapshot(create_filesystem(dst_root_dataset, "bar"), "b1")
        destroy(build(src_root_dataset + "/foo"), recursive=True)
        self.assertFalse(dataset_exists(src_root_dataset + "/foo"))
        self.assertTrue(dataset_exists(src_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + "/foo"))
        self.run_bzfs(src_root_dataset, dst_root_dataset, "--skip-replication", "--delete-dst-datasets")
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + "/foo"))
        self.assertTrue(dataset_exists(dst_root_dataset + "/bar"))

    def test_delete_dst_datasets_recursive1(self):
        self.setup_basic_with_recursive_replication_done()
        take_snapshot(create_filesystem(dst_root_dataset, "bar"), fix("b1"))
        take_snapshot(create_filesystem(dst_root_dataset, "zoo"), fix("z1"))
        destroy(build(src_root_dataset + "/foo"), recursive=True)
        self.assertFalse(dataset_exists(src_root_dataset + "/foo"))
        self.assertTrue(dataset_exists(src_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + "/foo"))
        self.run_bzfs(src_root_dataset, dst_root_dataset, "--recursive", "--skip-replication", "--delete-dst-datasets")
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/bar"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/zoo"))

    def test_delete_dst_datasets_with_exclude_regex1(self):
        self.setup_basic_with_recursive_replication_done()
        take_snapshot(create_filesystem(dst_root_dataset, "bar"), fix("b1"))
        take_snapshot(create_filesystem(dst_root_dataset, "zoo"), fix("z1"))
        destroy(build(src_root_dataset + "/foo"), recursive=True)
        self.assertFalse(dataset_exists(src_root_dataset + "/foo"))
        self.assertTrue(dataset_exists(src_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + "/foo"))
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--recursive",
            "--skip-replication",
            "--delete-dst-datasets",
            "--exclude-dataset-regex",
            "bar?",
            "--exclude-dataset-regex",
            "zoo*",
        )
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo"))
        self.assertTrue(dataset_exists(dst_root_dataset + "/bar"))
        self.assertTrue(dataset_exists(dst_root_dataset + "/zoo"))

    def test_delete_dst_datasets_with_exclude_regex2(self):
        self.setup_basic_with_recursive_replication_done()
        take_snapshot(create_filesystem(dst_root_dataset, "bar"), fix("b1"))
        take_snapshot(create_filesystem(dst_root_dataset, "zoo"), fix("z1"))
        destroy(build(src_root_dataset + "/foo"), recursive=True)
        self.assertFalse(dataset_exists(src_root_dataset + "/foo"))
        self.assertTrue(dataset_exists(src_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + "/foo"))
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--recursive",
            "--skip-replication",
            "--delete-dst-datasets",
            "--exclude-dataset-regex",
            "!bar",
        )
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + "/foo"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/bar"))
        self.assertTrue(dataset_exists(dst_root_dataset + "/zoo"))

    def test_delete_dst_datasets_with_exclude_dataset(self):
        self.setup_basic_with_recursive_replication_done()
        take_snapshot(create_filesystem(dst_root_dataset, "bar"), fix("b1"))
        take_snapshot(create_filesystem(dst_root_dataset, "zoo"), fix("z1"))
        destroy(build(src_root_dataset + "/foo"), recursive=True)
        self.assertFalse(dataset_exists(src_root_dataset + "/foo"))
        self.assertTrue(dataset_exists(src_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + "/foo"))
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--recursive",
            "--skip-replication",
            "--delete-dst-datasets",
            "--exclude-dataset",
            "foo",
            "--exclude-dataset",
            "zoo",
            "--exclude-dataset",
            "foo/b",
            "--exclude-dataset",
            "xxxxx",
        )
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + "/foo"))
        self.assertTrue(dataset_exists(dst_root_dataset + "/foo/a"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/bar"))
        self.assertTrue(dataset_exists(dst_root_dataset + "/zoo"))

    def test_delete_dst_datasets_and_empty_datasets(self):
        create_filesystems("axe")
        create_filesystems("foo/a")
        create_filesystems("foo/a/b")
        create_filesystems("foo/a/b/c")
        create_filesystems("foo/a/b/d")
        take_snapshot(create_filesystems("foo/a/e"), fix("e1"))
        create_filesystems("foo/b/c")
        create_filesystems("foo/b/c/d")
        create_filesystems("foo/b/d")
        take_snapshot(create_filesystems("foo/c"), fix("c1"))
        create_volumes("zoo")
        create_filesystems("boo")
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--recursive",
            "--skip-replication",
            "--delete-dst-datasets",
            "--delete-empty-dst-datasets",
            "--exclude-dataset",
            "boo",
        )
        self.assertFalse(dataset_exists(dst_root_dataset + "/axe"))
        self.assertTrue(dataset_exists(dst_root_dataset + "/foo/a"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/a/b"))
        self.assertTrue(dataset_exists(dst_root_dataset + "/foo/a/e"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))
        self.assertTrue(dataset_exists(dst_root_dataset + "/foo/c"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/zoo"))
        self.assertTrue(dataset_exists(dst_root_dataset + "/boo"))

    def test_delete_dst_snapshots_and_empty_datasets(self):
        create_filesystems("axe")
        create_filesystems("foo/a")
        create_filesystems("foo/a/b")
        create_filesystems("foo/a/b/c")
        create_filesystems("foo/a/b/d")
        take_snapshot(create_filesystems("foo/a/e"), fix("e1"))
        create_filesystems("foo/b/c")
        create_filesystems("foo/b/c/d")
        create_filesystems("foo/b/d")
        take_snapshot(create_filesystems("foo/c"), fix("c1"))
        create_volumes("zoo")
        create_filesystems("boo")
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--recursive",
            "--skip-replication",
            "--delete-empty-dst-datasets",
            "--delete-dst-snapshots",
            "--exclude-dataset",
            "boo",
        )
        self.assertFalse(dataset_exists(dst_root_dataset + "/axe"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/a"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/a/b"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/a/e"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/c"))
        self.assertFalse(dataset_exists(dst_root_dataset + "/zoo"))
        self.assertTrue(dataset_exists(dst_root_dataset + "/boo"))

    def test_delete_dst_snapshots_nothing_todo(self):
        self.setup_basic_with_recursive_replication_done()
        self.assertTrue(dataset_exists(src_root_dataset + "/foo/b"))
        self.assertEqual(0, len(snapshots(build(src_root_dataset + "/foo/b"))))
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))
        self.run_bzfs(
            src_root_dataset + "/foo/b", dst_root_dataset + "/foo/b", "--skip-replication", "--delete-dst-snapshots"
        )
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/b"))

    def test_delete_dst_bookmarks_flat_with_replication(self):
        self.setup_basic()
        self.run_bzfs(src_root_dataset, dst_root_dataset, "--delete-dst-snapshots=bookmarks")
        self.assertSnapshots(dst_root_dataset, 3, "s")

    def test_delete_dst_snapshots_flat_with_replication_with_crosscheck(self):
        self.setup_basic()
        for j in range(0, 2):
            for i in range(0, 3):
                with stop_on_failure_subtest(i=j * 3 + i):
                    destroy_snapshots(src_root_dataset, snapshots(src_root_dataset))
                    destroy_snapshots(dst_root_dataset, snapshots(dst_root_dataset))
                    if are_bookmarks_enabled("src"):
                        for bookmark in bookmarks(src_root_dataset):
                            destroy(bookmark)
                    take_snapshot(src_root_dataset, fix("s1"))
                    take_snapshot(src_root_dataset, fix("s2"))
                    take_snapshot(src_root_dataset, fix("s3"))
                    self.run_bzfs(src_root_dataset, dst_root_dataset)
                    self.assertSnapshots(dst_root_dataset, 3, "s")
                    if i > 0:
                        destroy_snapshots(src_root_dataset, snapshots(src_root_dataset))
                    if i > 1 and are_bookmarks_enabled("src"):
                        for bookmark in bookmarks(src_root_dataset):
                            destroy(bookmark)
                    crosscheck = [] if j == 0 else ["--delete-dst-snapshots-no-crosscheck"]
                    self.run_bzfs(
                        src_root_dataset, dst_root_dataset, "--skip-replication", "--delete-dst-snapshots", *crosscheck
                    )
                    if i == 0:
                        self.assertSnapshots(dst_root_dataset, 3, "s")
                    elif i == 1:
                        if j == 0 and are_bookmarks_enabled("src"):
                            self.assertSnapshotNames(dst_root_dataset, ["s1", "s3"])
                        else:
                            self.assertSnapshotNames(dst_root_dataset, [])
                    else:
                        self.assertSnapshotNames(dst_root_dataset, [])

    def test_delete_dst_snapshots_flat(self):
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                if i > 0:
                    self.tearDownAndSetup()
                self.setup_basic_with_recursive_replication_done()
                destroy(snapshots(src_root_dataset)[2])
                destroy(snapshots(src_root_dataset)[0])
                src_foo = build(src_root_dataset + "/foo")
                destroy(snapshots(src_foo)[1])
                src_foo_a = build(src_root_dataset + "/foo/a")
                destroy(snapshots(src_foo_a)[2])
                kwargs = {}
                if i != 0:
                    kwargs["max_command_line_bytes"] = 1
                self.run_bzfs(
                    src_root_dataset,
                    dst_root_dataset,
                    "--skip-replication",
                    "--delete-dst-snapshots",
                    "--delete-dst-snapshots-no-crosscheck",
                    **kwargs,
                )
                self.assertSnapshotNames(dst_root_dataset, ["s2"])
                self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")
                self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")

        if are_bookmarks_enabled("src"):
            tag0 = snapshot_name(snapshots(src_root_dataset)[0])
            create_bookmark(dst_root_dataset, tag0, tag0)
            for snap in snapshots(src_root_dataset, max_depth=None) + snapshots(dst_root_dataset, max_depth=None):
                destroy(snap)
            self.run_bzfs(
                src_root_dataset, dst_root_dataset, "--skip-replication", "--delete-empty-dst-datasets", "--recursive"
            )
            self.assertTrue(dataset_exists(dst_root_dataset))
        else:
            for snap in snapshots(src_root_dataset, max_depth=None) + snapshots(dst_root_dataset, max_depth=None):
                destroy(snap)

        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--skip-replication",
            "--delete-empty-dst-datasets=snapshots",
            "--recursive",
        )
        self.assertFalse(dataset_exists(dst_root_dataset))

    def test_delete_dst_bookmarks_flat(self):
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        for i in range(0, 1):
            with stop_on_failure_subtest(i=i):
                if i > 0:
                    self.tearDownAndSetup()
                self.setup_basic()
                self.run_bzfs(src_root_dataset, dst_root_dataset)
                tag0 = snapshot_name(snapshots(src_root_dataset)[0])
                create_bookmark(dst_root_dataset, tag0, tag0)
                tag1 = snapshot_name(snapshots(src_root_dataset)[1])
                create_bookmark(dst_root_dataset, tag1, tag1)
                tag2 = snapshot_name(snapshots(src_root_dataset)[2])
                create_bookmark(dst_root_dataset, tag2, tag2)
                self.assertBookmarkNames(src_root_dataset, ["s1", "s3"])
                self.assertBookmarkNames(dst_root_dataset, ["s1", "s2", "s3"])
                for snapshot in snapshots(src_root_dataset) + snapshots(dst_root_dataset):
                    destroy(snapshot)
                self.run_bzfs(
                    src_root_dataset, dst_root_dataset, "--skip-replication", "--delete-dst-snapshots=bookmarks"
                )
                self.assertBookmarkNames(dst_root_dataset, ["s1", "s3"])

                self.run_bzfs(
                    src_root_dataset, dst_root_dataset, "--skip-replication", "--delete-dst-snapshots=bookmarks"
                )
                self.assertBookmarkNames(dst_root_dataset, ["s1", "s3"])

                for bookmark in bookmarks(src_root_dataset):
                    destroy(bookmark)
                self.run_bzfs(
                    src_root_dataset,
                    dst_root_dataset,
                    "--skip-replication",
                    "--delete-dst-snapshots=bookmarks",
                    "--delete-empty-dst-datasets",
                )
                self.assertBookmarkNames(dst_root_dataset, [])
                self.assertTrue(dataset_exists(dst_root_dataset))

                self.run_bzfs(
                    src_root_dataset,
                    dst_root_dataset,
                    "--skip-replication",
                    "--delete-empty-dst-datasets",
                    "--recursive",
                )
                self.assertFalse(dataset_exists(dst_root_dataset))

    def test_delete_dst_snapshots_despite_same_name(self):
        self.setup_basic_with_recursive_replication_done()
        destroy(snapshots(src_root_dataset)[2])
        destroy(snapshots(src_root_dataset)[0])
        take_snapshot(src_root_dataset, fix("s1"))  # Note: not the same as prior snapshot (has different GUID)
        take_snapshot(src_root_dataset, fix("s3"))  # Note: not the same as prior snapshot (has different GUID)
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
        )
        self.assertSnapshotNames(dst_root_dataset, ["s2"])

    def test_delete_dst_snapshots_recursive(self):
        self.setup_basic_with_recursive_replication_done()
        destroy(snapshots(src_root_dataset)[2])
        destroy(snapshots(src_root_dataset)[0])
        src_foo = build(src_root_dataset + "/foo")
        destroy(snapshots(src_foo)[1])
        src_foo_a = build(src_root_dataset + "/foo/a")
        destroy(src_foo_a, recursive=True)
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--recursive",
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
        )
        self.assertSnapshotNames(dst_root_dataset, ["s2"])
        self.assertSnapshotNames(dst_root_dataset + "/foo", ["t1", "t3"])
        self.assertTrue(dataset_exists(dst_root_dataset + "/foo/a"))
        self.assertSnapshotNames(dst_root_dataset + "/foo/a", [])

    def test_delete_dst_snapshots_recursive_with_delete_empty_dst_datasets(self):
        self.setup_basic_with_recursive_replication_done()
        destroy(snapshots(src_root_dataset)[2])
        destroy(snapshots(src_root_dataset)[0])
        src_foo = build(src_root_dataset + "/foo")
        destroy(snapshots(src_foo)[1])
        src_foo_a = build(src_root_dataset + "/foo/a")
        destroy(src_foo_a, recursive=True)
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--recursive",
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            "--delete-empty-dst-datasets",
        )
        self.assertSnapshotNames(dst_root_dataset, ["s2"])
        self.assertSnapshotNames(dst_root_dataset + "/foo", ["t1", "t3"])
        self.assertFalse(dataset_exists(dst_root_dataset + "/foo/a"))

    def test_delete_dst_snapshots_recursive_with_delete_empty_dst_datasets_with_dummy(self):
        self.setup_basic_with_recursive_replication_done()
        for i in range(0, 3):
            self.run_bzfs(
                bzfs.dummy_dataset,
                dst_root_dataset,
                "--recursive",
                "--skip-replication",
                "--delete-dst-snapshots",
                "--delete-dst-snapshots-no-crosscheck",
                "--delete-empty-dst-datasets",
                dry_run=(i == 0),
            )
            if i == 0:
                self.assertSnapshots(dst_root_dataset, 3, "s")
            else:
                self.assertFalse(dataset_exists(dst_root_dataset))

    def test_delete_dst_snapshots_flat_with_nonexisting_destination(self):
        self.setup_basic()
        destroy(dst_root_dataset, recursive=True)
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
        )
        self.assertFalse(dataset_exists(dst_root_dataset))

    def test_delete_dst_snapshots_recursive_with_injected_errors(self):
        self.setup_basic_with_recursive_replication_done()
        self.assertTrue(dataset_exists(dst_root_dataset))

        # inject send failures before this many tries. only after that succeed the operation
        counter = Counter(zfs_delete_snapshot=2)
        self.run_bzfs(
            bzfs.dummy_dataset,
            dst_root_dataset,
            "--skip-replication",
            "--recursive",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            retries=2,
            error_injection_triggers={"before": counter},
        )
        self.assertEqual(0, counter["zfs_delete_snapshot"])
        self.assertEqual(0, len(snapshots(dst_root_dataset, max_depth=None)))

    def test_delete_dst_snapshots_recursive_with_injected_dataset_deletes(self):
        self.setup_basic_with_recursive_replication_done()
        self.assertTrue(dataset_exists(dst_root_dataset))

        # inject deletes for this many times. only after that stop deleting datasets
        counter = Counter(zfs_list_delete_dst_snapshots=1)
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--skip-replication",
            "--recursive",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            delete_injection_triggers={"before": counter},
        )
        self.assertEqual(0, counter["zfs_list_delete_dst_snapshots"])
        self.assertFalse(dataset_exists(dst_root_dataset))

    def test_delete_dst_snapshots_flat_with_time_range_full(self):
        self.setup_basic_with_recursive_replication_done()
        destroy(snapshots(src_root_dataset)[2])
        destroy(snapshots(src_root_dataset)[0])
        src_foo = build(src_root_dataset + "/foo")
        destroy(snapshots(src_foo)[1])
        src_foo_a = build(src_root_dataset + "/foo/a")
        destroy(snapshots(src_foo_a)[2])
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            "--include-snapshot-times-and-ranks=60secs ago..2999-01-01",
        )
        self.assertSnapshotNames(dst_root_dataset, ["s2"])
        self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")
        self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")

    def test_delete_dst_snapshots_flat_with_time_range_empty(self):
        self.setup_basic_with_recursive_replication_done()
        destroy(snapshots(src_root_dataset)[2])
        destroy(snapshots(src_root_dataset)[0])
        src_foo = build(src_root_dataset + "/foo")
        destroy(snapshots(src_foo)[1])
        src_foo_a = build(src_root_dataset + "/foo/a")
        destroy(snapshots(src_foo_a)[2])
        kwargs = {}
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            "--include-snapshot-times-and-ranks=2999-01-01..2999-01-01",
            **kwargs,
        )
        self.assertSnapshots(dst_root_dataset, 3, "s")
        self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")
        self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")

    def test_delete_dst_snapshots_with_excludes_flat_nothing_todo(self):
        self.setup_basic_with_recursive_replication_done()
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            "--exclude-snapshot-regex",
            "xxxx*",
        )
        self.assertSnapshots(dst_root_dataset, 3, "s")
        self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")
        self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")

    def test_delete_dst_snapshots_with_excludes_flat(self):
        self.setup_basic_with_recursive_replication_done()
        for snap in snapshots(src_root_dataset):
            destroy(snap)
        for snap in snapshots(build(src_root_dataset + "/foo")):
            destroy(snap)
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            "--exclude-snapshot-regex",
            r"!.*s[1-2]+.*",
        )
        self.assertSnapshotNames(dst_root_dataset, ["s3"])
        self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")
        self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")

    def test_delete_dst_snapshots_with_excludes_recursive(self):
        self.setup_basic_with_recursive_replication_done()
        for snap in snapshots(src_root_dataset):
            destroy(snap)
        for snap in snapshots(build(src_root_dataset + "/foo")):
            destroy(snap)
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--recursive",
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            "--delete-empty-dst-datasets",
            "--exclude-snapshot-regex",
            ".*s[1-2]+.*",
            "--exclude-snapshot-regex",
            ".*t1.*",
            "--exclude-snapshot-regex",
            ".*u.*",
        )
        self.assertSnapshotNames(dst_root_dataset, ["s1", "s2"])
        self.assertSnapshotNames(dst_root_dataset + "/foo", ["t1"])
        self.assertSnapshotNames(dst_root_dataset + "/foo/a", ["u1", "u2", "u3"])

    def test_delete_dst_snapshots_with_excludes_recursive_and_excluding_dataset_regex(self):
        self.setup_basic_with_recursive_replication_done()
        for snap in snapshots(src_root_dataset):
            destroy(snap)
        for snap in snapshots(build(src_root_dataset + "/foo")):
            destroy(snap)
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--recursive",
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            "--delete-empty-dst-datasets",
            "--exclude-dataset-regex",
            "foo",
            "--exclude-snapshot-regex",
            ".*s[1-2]+.*",
            "--exclude-snapshot-regex",
            ".*t1.*",
            "--exclude-snapshot-regex",
            ".*u1.*",
        )
        self.assertSnapshotNames(dst_root_dataset, ["s1", "s2"])
        self.assertSnapshots(dst_root_dataset + "/foo", 3, "t")
        self.assertSnapshots(dst_root_dataset + "/foo/a", 3, "u")

    def test_syslog(self):
        if "Ubuntu" not in platform.version():
            self.skipTest("It is sufficient to only test this on Ubuntu where syslog paths are well known")
        for i in range(0, 2):
            if i > 0:
                self.tearDownAndSetup()
            with stop_on_failure_subtest(i=i):
                syslog_prefix = "bzfs_backup"
                verbose = ["-v"] if i == 0 else []
                self.run_bzfs(
                    src_root_dataset,
                    dst_root_dataset,
                    *verbose,
                    "--log-syslog-address=/dev/log",
                    "--log-syslog-socktype=UDP",
                    "--log-syslog-facility=2",
                    "--log-syslog-level=TRACE",
                    "--log-syslog-prefix=" + syslog_prefix,
                    "--skip-replication",
                )
                lines = list(bzfs.tail("/var/log/syslog", 100))
                k = -1
                for kk, line in enumerate(lines):
                    if syslog_prefix in line and "Log file is:" in line:
                        k = kk
                self.assertGreaterEqual(k, 0)
                lines = lines[k:]
                found_msg = False
                for line in lines:
                    if syslog_prefix in line:
                        if i == 0:
                            found_msg = found_msg or " [T] " in line
                        else:
                            found_msg = found_msg or " [D] " in line
                            self.assertNotIn(" [T] ", line)
                self.assertTrue(found_msg, "No bzfs syslog message was found")

    def test_log_config_file_nonempty(self):
        if "Ubuntu" not in platform.version():
            self.skipTest("It is sufficient to only test this on Ubuntu where syslog paths are well known")
        config_str = """
# This is an example log_config.json file that demonstrates how to configure bzfs logging via the standard
# python logging.config.dictConfig mechanism.
#
# For more examples see
# https://stackoverflow.com/questions/7507825/where-is-a-complete-example-of-logging-config-dictconfig
# and for details see https://docs.python.org/3/library/logging.config.html#configuration-dictionary-schema
#
# Note: Lines starting with a # character are ignored as comments within the JSON.
# Also, if a line ends with a # character the portion between that # character and the preceding # character on
# the same line is ignored as a comment.
#
# User defined variables and their values can be specified via the --log-config-var=name:value CLI option. These
# variables can be used in the JSON config via ${name[:default]} references, which are substituted (aka interpolated)
# as follows:
# If the variable contains a non-empty CLI value then that value is used. Else if a default value for the
# variable exists in the JSON file that default value is used. Else the program aborts with an error.
# Example: In the JSON variable ${syslog_address:/dev/log}, the variable name is "syslog_address"
# and the default value is "/dev/log". The default value is the portion after the optional : colon within the
# variable declaration. The default value is used if the CLI user does not specify a non-empty value via
# --log-config-var, for example via
# --log-config-var syslog_address:/path/to/socket_file
#
# bzfs automatically supplies the following convenience variables:
# ${bzfs.log_level}, ${bzfs.log_dir}, ${bzfs.log_file}, ${bzfs.sub.logger},
# ${bzfs.get_default_log_formatter}, ${bzfs.timestamp}.
# For a complete list see the source code of get_dict_config_logger().
{
    "version": 1,
    "disable_existing_loggers": false,
    "formatters": {  # formatters specify how to convert a log record to a string message #
        "bzfs": {
            # () specifies factory function to call in order to return a formatter.
            "()": "${bzfs.get_default_log_formatter}"
        },
        "bzfs_syslog": {
            # () specifies factory function to call with the given prefix arg in order to return a formatter.
            # The prefix identifies bzfs messages within the syslog, as opposed to messages from other sources.
            "()": "${bzfs.get_default_log_formatter}",
            "prefix": "bzfs.sub "
        },
        "simple": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },
    "handlers": {  # handlers specify where to write messages to #
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "simple",
            # "formatter": "bzfs",
            "stream": "ext://sys.stdout"  # log to stdout #
        },
        "file": {
            "class": "logging.FileHandler",
            "formatter": "bzfs",
            "filename": "${bzfs.log_dir}/${log_file_prefix:custom-}${bzfs.log_file}",  # log to this output file #
            "encoding": "utf-8"
        },
        "syslog": {
            "class": "logging.handlers.SysLogHandler",  # log to local or remote syslog #
            "level": "${syslog_level:INFO}",  # fall back to INFO level if syslog_level variable is empty #
            "formatter": "bzfs_syslog",
            "address": "${syslog_address:/dev/log}",  # log to local syslog socket file #
            # "address": ["${syslog_host:127.0.0.1}", ${syslog_port:514}],  # log to remote syslog #
            "socktype": "ext://socket.SOCK_DGRAM"  # Refers to existing UDP python object #
            # "socktype": "ext://socket.SOCK_STREAM"  # Refers to existing TCP python object #
        }
    },
    "loggers": {  # loggers specify what log records to forward to which handlers #
        "${bzfs.sub.logger}": {
            "level": "${log_level:TRACE}",  # do not forward any log record below that level #
            "handlers": ["console", "file", "syslog"]  # forward records to these handlers, which format and print em #
            # "handlers": ["file", "syslog"]  # use default console handler instead of a custom handler #
        }
    }
}
        """
        for i in range(0, 2):
            if i > 0:
                self.tearDownAndSetup()
            log_file_prefix = "custom-" if i == 0 else ""
            self.run_bzfs(
                src_root_dataset,
                dst_root_dataset,
                "--log-config-file=" + (config_str if i == 0 else config_str.replace("custom-", "")),
                "--log-config-var",
                "syslog_address:/dev/log",
                "log_file_prefix:" + log_file_prefix,
                "--skip-replication",
            )
        output_dir = os.path.dirname(os.path.abspath(__file__))
        if os.access(output_dir, os.W_OK):
            with open(os.path.join(output_dir, "log_config.json"), "w", encoding="utf-8") as fd:
                fd.write(config_str.lstrip())

    def test_log_config_file_empty(self):
        if "Ubuntu" not in platform.version():
            self.skipTest("It is sufficient to only test this on Ubuntu where syslog paths are well known")
        config_str = """ "version": 1, "disable_existing_loggers": false, "foo": "${bar:}" """
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            "--log-config-file=" + config_str,
            "--log-config-var",
            "syslog_address:/dev/log",
            "bar:white\t\n space",
            "--skip-replication",
            "-v",
            "-v",
        )

        # test reading from file instead of string
        tmpfile_fd, tmpfile = tempfile.mkstemp(prefix="test_bzfs.config_file")
        os.write(tmpfile_fd, config_str.encode("utf-8"))
        os.close(tmpfile_fd)
        try:
            self.run_bzfs(
                src_root_dataset,
                dst_root_dataset,
                "--log-config-file=+" + tmpfile,
                "--log-config-var",
                "syslog_address:/dev/log",
                "bar:white\t\n space",
                "--skip-replication",
            )
        finally:
            os.remove(tmpfile)

    def test_log_config_file_error(self):
        if "Ubuntu" not in platform.version():
            self.skipTest("It is sufficient to only test this on Ubuntu where syslog paths are well known")

        # test that a trailing hash without a preceding hash is not ignored as a comment, and hence leads to a
        # JSON parser error
        config_str = """{ "version": 1, "disable_existing_loggers": false }#"""
        with self.assertRaises(json.decoder.JSONDecodeError):
            self.run_bzfs(
                src_root_dataset,
                dst_root_dataset,
                "--log-config-file=" + config_str,
                "--skip-replication",
            )

        # Missing default value for empty substitution variable
        config_str = """{ "version": 1, "disable_existing_loggers": false, "foo": "${missing_var}" }"""
        with self.assertRaises(ValueError):
            self.run_bzfs(
                src_root_dataset,
                dst_root_dataset,
                "--log-config-file=" + config_str,
                "--skip-replication",
            )

        # User defined name:value variable must not be empty
        config_str = """{ "version": 1, "disable_existing_loggers": false, "foo": "${:}" }"""
        with self.assertRaises(ValueError):
            self.run_bzfs(
                src_root_dataset,
                dst_root_dataset,
                "--log-config-file=" + config_str,
                "--skip-replication",
            )

    def test_main(self):
        self.setup_basic()
        with self.assertRaises(SystemExit):
            bzfs.main()
        with self.assertRaises(SystemExit):
            bzfs.run_main(bzfs.argument_parser().parse_args(["xxxx", dst_root_dataset]))
        bzfs.run_main(bzfs.argument_parser().parse_args([src_root_dataset, dst_root_dataset]))

    def test_program_name_must_not_contain_whitespace(self):
        self.run_bzfs(src_root_dataset, dst_root_dataset, "--zfs-program=zfs zfs", expected_status=die_status)

    def test_ssh_program_must_not_be_disabled_in_nonlocal_mode(self):
        if not self.param or self.param.get("ssh_mode", "local") == "local" or ssh_program != "ssh":
            self.skipTest("ssh is only required in nonlocal mode")
        self.run_bzfs(
            src_root_dataset, dst_root_dataset, "--ssh-program=" + bzfs.disable_prg, expected_status=die_status
        )


#############################################################################
class MinimalRemoteTestCase(BZFSTestCase):
    def test_basic_replication_flat_simple(self):
        LocalTestCase(param=self.param).test_basic_replication_flat_simple()

    def test_basic_replication_recursive1(self):
        LocalTestCase(param=self.param).test_basic_replication_recursive1()

    def test_delete_dst_datasets_recursive_with_dummy_src(self):
        LocalTestCase(param=self.param).test_delete_dst_datasets_recursive_with_dummy_src()

    def test_inject_unavailable_sudo(self):
        expected_error = die_status if os.geteuid() != 0 and not self.is_no_privilege_elevation() else 0
        self.inject_unavailable_program("inject_unavailable_sudo", expected_error=expected_error)
        self.tearDownAndSetup()
        expected_error = 1 if os.geteuid() != 0 and not self.is_no_privilege_elevation() else 0
        self.inject_unavailable_program("inject_failing_sudo", expected_error=expected_error)

    def test_disabled_sudo(self):
        expected_status = 0
        if os.geteuid() != 0 and not self.is_no_privilege_elevation():
            expected_status = die_status
        self.inject_disabled_program("sudo", expected_error=expected_status)

    def inject_disabled_program(self, prog, expected_error=0):
        self.setup_basic()
        self.run_bzfs(
            src_root_dataset,
            dst_root_dataset,
            f"--{prog}-program=" + bzfs.disable_prg,
            expected_status=expected_error,
        )
        if expected_error != 0:
            self.assertSnapshots(dst_root_dataset, 0)

    def inject_unavailable_program(self, *flags, expected_error=0):
        self.setup_basic()
        inject_params = {}
        for flag in flags:
            inject_params[flag] = True
        self.run_bzfs(src_root_dataset, dst_root_dataset, expected_status=expected_error, inject_params=inject_params)
        if expected_error != 0:
            self.assertSnapshots(dst_root_dataset, 0)
        else:
            self.assertSnapshots(dst_root_dataset, 3, "s")


#############################################################################
class FullRemoteTestCase(MinimalRemoteTestCase):

    def test_ssh_program_must_not_be_disabled_in_nonlocal_mode(self):
        LocalTestCase(param=self.param).test_ssh_program_must_not_be_disabled_in_nonlocal_mode()

    def test_basic_replication_flat_nothing_todo(self):
        LocalTestCase(param=self.param).test_basic_replication_flat_nothing_todo()

    def test_basic_replication_without_source(self):
        LocalTestCase(param=self.param).test_basic_replication_without_source()

    def test_complex_replication_flat_use_bookmarks(self):
        LocalTestCase(param=self.param).test_complex_replication_flat_use_bookmarks()

    def test_basic_replication_flat_send_recv_flags(self):
        LocalTestCase(param=self.param).test_basic_replication_flat_send_recv_flags()

    def test_basic_replication_flat_simple_with_multiple_root_datasets(self):
        LocalTestCase(param=self.param).test_basic_replication_flat_simple_with_multiple_root_datasets()

    def test_basic_replication_dataset_with_spaces(self):
        LocalTestCase(param=self.param).test_basic_replication_dataset_with_spaces()

    def test_basic_replication_flat_with_multiple_root_datasets_converted_from_recursive(self):
        LocalTestCase(
            param=self.param
        ).test_basic_replication_flat_with_multiple_root_datasets_converted_from_recursive()

    def test_zfs_set(self):
        LocalTestCase(param=self.param).test_zfs_set()

    def test_zfs_set_via_recv_o(self):
        LocalTestCase(param=self.param).test_zfs_set_via_recv_o()

    def test_zfs_set_via_set_include(self):
        LocalTestCase(param=self.param).test_zfs_set_via_set_include()

    def test_inject_src_pipe_fail(self):
        self.inject_pipe_error("inject_src_pipe_fail", expected_error=[1, die_status])

    def test_inject_src_pipe_garble(self):
        self.inject_pipe_error("inject_src_pipe_garble")

    def test_inject_dst_pipe_garble(self):
        self.inject_pipe_error("inject_dst_pipe_garble")

    def test_inject_src_send_error(self):
        self.inject_pipe_error("inject_src_send_error")

    def test_inject_dst_receive_error(self):
        self.inject_pipe_error("inject_dst_receive_error", expected_error=2)

    def inject_pipe_error(self, flag, expected_error=1):
        self.setup_basic()
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                inject_params = {}
                if i == 0:
                    inject_params[flag] = True
                self.run_bzfs(
                    src_root_dataset,
                    dst_root_dataset,
                    expected_status=expected_error if i == 0 else 0,
                    inject_params=inject_params,
                )
                if i == 0:
                    self.assertSnapshots(dst_root_dataset, 0)
                else:
                    self.assertSnapshots(dst_root_dataset, 3, "s")

    def test_inject_unavailable_mbuffer(self):
        self.inject_unavailable_program("inject_unavailable_mbuffer")
        if self.param and self.param.get("ssh_mode") != "local" and self.param.get("min_pipe_transfer_size", -1) == 0:
            self.tearDownAndSetup()
            self.inject_unavailable_program("inject_failing_mbuffer", expected_error=1)

    def test_inject_unavailable_ps(self):
        self.inject_unavailable_program("inject_unavailable_ps")

    def test_inject_unavailable_pv(self):
        self.inject_unavailable_program("inject_unavailable_pv")

    def test_inject_unavailable_sh(self):
        self.inject_unavailable_program("inject_unavailable_sh")
        self.tearDownAndSetup()
        self.inject_unavailable_program("inject_failing_sh")

    def test_inject_unavailable_zstd(self):
        self.inject_unavailable_program("inject_unavailable_zstd")

    def test_inject_unavailable_zpool(self):
        self.inject_unavailable_program("inject_unavailable_zpool")
        self.tearDownAndSetup()
        self.inject_unavailable_program("inject_failing_zpool")

    def test_inject_unavailable_ssh(self):
        if self.param and self.param.get("ssh_mode") != "local":
            self.inject_unavailable_program("inject_unavailable_" + ssh_program, expected_error=die_status)
            self.tearDownAndSetup()
            self.inject_unavailable_program("inject_failing_" + ssh_program, expected_error=die_status)

    def test_inject_unavailable_zfs(self):
        self.inject_unavailable_program("inject_unavailable_zfs", expected_error=die_status)
        self.tearDownAndSetup()
        self.inject_unavailable_program("inject_failing_zfs", expected_error=die_status)

    def test_disabled_mbuffer(self):
        self.inject_disabled_program("mbuffer")

    def test_disabled_ps(self):
        self.inject_disabled_program("ps")

    def test_disabled_pv(self):
        self.inject_disabled_program("pv")

    def test_disabled_sh(self):
        self.inject_disabled_program("shell")

    def test_disabled_compression(self):
        self.inject_disabled_program("compression")

    def test_disabled_zpool(self):
        self.inject_disabled_program("zpool")


#############################################################################
def create_filesystems(path, props=None):
    create_filesystem(src_root_dataset, path, props=props)
    return create_filesystem(dst_root_dataset, path, props=props)


def recreate_filesystem(dataset, props=None):
    if dataset_exists(dataset):
        destroy(dataset, recursive=True)
    return create_filesystem(dataset, props=props)


def create_volumes(path, props=None):
    create_volume(src_root_dataset, path, size="1M", props=props)
    return create_volume(dst_root_dataset, path, size="1M", props=props)


def detect_zpool_features(location, pool):
    cmd = "zpool get -Hp -o property,value all".split(" ") + [pool]
    lines = run_cmd(cmd)
    props = {line.split("\t", 1)[0]: line.split("\t", 1)[1] for line in lines}
    features = {k: v for k, v in props.items() if k.startswith("feature@")}
    zpool_features[location] = features


def is_zpool_feature_enabled_or_active(location, feature):
    return zpool_features[location].get(feature) in ("active", "enabled")


def are_bookmarks_enabled(location):
    return is_zpool_feature_enabled_or_active(location, "feature@bookmark_v2") and is_zpool_feature_enabled_or_active(
        location, "feature@bookmark_written"
    )


def is_zpool_recv_resume_feature_enabled_or_active():
    return is_zpool_feature_enabled_or_active("src", "feature@extensible_dataset") and is_zfs_at_least_2_1_0()


def fix(s):
    """Generate names containing leading and trailing whitespace, forbidden characters, etc."""
    return afix + s + afix


def natsorted(iterable, key=None, reverse=False):
    """
    Returns a new list containing all items from the iterable in ascending order.
    If `key` is specified, it will be used to extract a comparison key from each list element.
    """
    if key is None:
        return sorted(iterable, key=natsort_key, reverse=reverse)
    else:
        return sorted(iterable, key=lambda x: natsort_key(key(x)), reverse=reverse)


def natsort_key(s: str):
    """Sorts strings that may contain non-negative integers according to numerical value if any two strings
    have the same non-numeric prefix. Example: s1 < s3 < s10 < s10a < s10b"""
    match = re.fullmatch(r"(\D*)(\d*)(.*)", s)
    if match:
        prefix, num, suffix = match.groups()
        num = int(num) if num else 0
        return prefix, num, suffix
    return s, 0


def is_solaris_zfs_at_least_11_4_42():
    return is_solaris_zfs() and bzfs.is_version_at_least(".".join(platform.version().split(".")[0:3]), "11.4.42")


def is_zfs_at_least_2_3_0():
    if is_solaris_zfs():
        return False
    ver = zfs_version()
    if ver is None:
        return False
    return bzfs.is_version_at_least(ver, "2.3.0")


def is_zfs_at_least_2_1_0():
    if is_solaris_zfs():
        return False
    ver = zfs_version()
    if ver is None:
        return False
    return bzfs.is_version_at_least(ver, "2.1.0")


def os_username():
    # return getpass.getuser()
    return pwd.getpwuid(os.getuid()).pw_name
