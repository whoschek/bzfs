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
"""Shared environment and base classes for integration tests.

Holds the integration test globals (e.g., temp ZFS key material, pool/dataset state) and common helpers so that split-out
integration test modules can share a single runtime context.
"""

from __future__ import (
    annotations,
)
import logging
import os
import platform
import pwd
import random
import re
import shutil
import subprocess
import tempfile
import threading
import traceback
import unittest
from collections import (
    Counter,
)
from collections.abc import (
    Iterable,
)
from pathlib import (
    Path,
)
from subprocess import (
    PIPE,
)
from typing import (
    Any,
    Callable,
    Final,
    cast,
)

from bzfs_main import (
    argparse_cli,
    bzfs,
    bzfs_jobrunner,
)
from bzfs_main.detect import (
    is_version_at_least,
)
from bzfs_main.replication import (
    INJECT_DST_PIPE_FAIL_KBYTES,
)
from bzfs_main.util.utils import (
    ENV_VAR_PREFIX,
    FILE_PERMISSIONS,
    find_match,
    get_home_directory,
    getenv_any,
    getenv_bool,
)
from bzfs_tests.abstract_testcase import (
    AbstractTestCase,
)
from bzfs_tests.zfs_util import (
    bookmark_name,
    bookmarks,
    build,
    create_filesystem,
    create_volume,
    dataset_exists,
    dataset_property,
    destroy,
    destroy_pool,
    run_cmd,
    set_sudo_cmd,
    snapshot_name,
    snapshots,
    take_snapshot,
    zfs_version,
)

# constants:
SRC_POOL_NAME: Final[str] = "wb_src"
DST_POOL_NAME: Final[str] = "wb_dst"
POOL_SIZE_BYTES_DEFAULT: Final[int] = 100 * 1024 * 1024
ENCRYPTION_ALGO: Final[str] = "aes-256-gcm"
CREATION_PREFIX: Final[str] = "bzfs_test:"

# Global variables populated during setUp()
SRC_POOL: str = ""
DST_POOL: str = ""
SRC_ROOT_DATASET: str = ""
DST_ROOT_DATASET: str = ""
AFIX: str = ""
ZPOOL_FEATURES: dict[str, dict[str, str]] | None = None

ZFS_ENCRYPTIONK_KEY_FD, ZFS_ENCRYPTION_KEY = tempfile.mkstemp(prefix="test_bzfs.key_")
os.write(ZFS_ENCRYPTIONK_KEY_FD, "mypasswd".encode("utf-8"))
os.close(ZFS_ENCRYPTIONK_KEY_FD)
KEYLOCATION: Final[str] = f"file://{ZFS_ENCRYPTION_KEY}"

SSH_CONFIG_FILE: Final[str] = os.path.join(get_home_directory(), ".ssh", "test_bzfs_ssh_config")
RNG: Final[random.Random] = random.Random(12345)
HAS_NETCAT_PROG: Final[bool] = shutil.which("nc") is not None

SSH_PROGRAM: Final[str] = cast(str, getenv_any("test_ssh_program", "ssh"))  # also works with "hpnssh"
SUDO_CMD: Final[list[str]] = ["sudo", "-n"] if getenv_bool("test_enable_sudo", True) and os.getuid() != 0 else []
set_sudo_cmd(SUDO_CMD)


#############################################################################
class ParametrizedTestCase(AbstractTestCase):

    def __init__(self, methodName: str = "runTest", param: dict[str, Any] | None = None) -> None:  # noqa: N803
        super().__init__(methodName)
        self.param = param

    @staticmethod
    def parametrize(testcase_klass: type, param: dict[str, Any] | None = None) -> unittest.TestSuite:
        testloader = unittest.TestLoader()
        testnames = testloader.getTestCaseNames(testcase_klass)
        suite = unittest.TestSuite()
        for name in testnames:
            suite.addTest(testcase_klass(name, param=param))
        return suite


#############################################################################
class IntegrationTestCase(ParametrizedTestCase):

    def setUp(self, pool_size_bytes: int = POOL_SIZE_BYTES_DEFAULT) -> None:
        global SRC_POOL, DST_POOL
        global SRC_ROOT_DATASET, DST_ROOT_DATASET
        global AFIX

        logdir = self.log_dir_opt()[-1]
        if os.path.exists(logdir):
            shutil.rmtree(logdir)
        for pool in SRC_POOL_NAME, DST_POOL_NAME:
            if dataset_exists(pool):
                destroy_pool(pool)
            if not dataset_exists(pool):
                tmp = tempfile.NamedTemporaryFile()  # noqa: SIM115
                tmp.seek(pool_size_bytes - 1)
                tmp.write(b"0")
                tmp.seek(0)
                run_cmd(SUDO_CMD + ["zpool", "create", "-O", "atime=off", pool, tmp.name])

        SRC_POOL = build(SRC_POOL_NAME)
        DST_POOL = build(DST_POOL_NAME)
        AFIX = self.param.get("affix", "") if self.param is not None else ""
        SRC_ROOT_DATASET = recreate_filesystem(SRC_POOL_NAME + "/tmp/" + fix("src"))
        DST_ROOT_DATASET = recreate_filesystem(DST_POOL_NAME + "/tmp/" + fix("dst"))

        global ZPOOL_FEATURES
        if ZPOOL_FEATURES is None:
            ZPOOL_FEATURES = {}
            detect_zpool_features("src", SRC_POOL_NAME)
            logging.getLogger(__name__).warning(f"zpool bookmarks feature: {are_bookmarks_enabled('src')}")
            props = ZPOOL_FEATURES["src"]
            features = "\n".join(
                [f"{k}: {v}" for k, v in sorted(props.items()) if k.startswith("feature@") or k == "delegation"]
            )
            self.assertIsNotNone(features)
            # print(f"test zpool features: {features}", file=sys.stderr)

        private_key_file = "id_rsa" if platform.system() == "Linux" else "testid_rsa"
        private_key_file = os.path.join(os.path.dirname(SSH_CONFIG_FILE), private_key_file)
        fallback = "Fallback=no\n" if SSH_PROGRAM == "hpnssh" else ""  # see https://www.psc.edu/hpn-ssh-home/hpn-readme/
        with open(SSH_CONFIG_FILE, "w", encoding="utf-8") as fd:
            fd.write(f"IdentityFile {private_key_file}\nStrictHostKeyChecking=no\n{fallback}Include /etc/ssh/ssh_config\n")
        os.chmod(SSH_CONFIG_FILE, mode=FILE_PERMISSIONS)  # chmod u=rw,go=

    # zpool list -o name|grep '^wb_'|xargs -n 1 -r --verbose zpool destroy; rm -fr /tmp/tmp* /run/user/$UID/bzfs/
    def tearDown(self) -> None:
        Path(SSH_CONFIG_FILE).unlink(missing_ok=True)
        # tear down is deferred to next setup for easier debugging

    def tearDownAndSetup(self) -> None:  # noqa: N802
        self.tearDown()
        self.setUp()

    def setup_basic(self, volume: bool = False) -> None:
        compression_props = ["-o", "compression=on"]
        encryption_props = ["-o", f"encryption={ENCRYPTION_ALGO}"]
        encryption_props += ["-o", "keyformat=passphrase", "-o", f"keylocation={KEYLOCATION}"]

        dataset_props = encryption_props + compression_props if self.is_encryption_mode() else compression_props
        src_foo = create_filesystem(SRC_ROOT_DATASET, "foo", props=dataset_props)
        src_foo_a = create_volume(src_foo, "a", size="1M") if volume else create_filesystem(src_foo, "a")
        src_foo_b = create_filesystem(src_foo, "b")
        self.assertIsNotNone(src_foo_b)
        take_snapshot(SRC_ROOT_DATASET, fix("s1"))
        take_snapshot(SRC_ROOT_DATASET, fix("s2"))
        take_snapshot(SRC_ROOT_DATASET, fix("s3"))
        take_snapshot(src_foo, fix("t1"))
        take_snapshot(src_foo, fix("t2"))
        take_snapshot(src_foo, fix("t3"))
        take_snapshot(src_foo_a, fix("u1"))
        take_snapshot(src_foo_a, fix("u2"))
        take_snapshot(src_foo_a, fix("u3"))

    def setup_basic_woo(self, w: int = 1, q: int = 0) -> None:
        for i in range(w):
            src_woo = create_filesystem(SRC_ROOT_DATASET, f"woo{i}")
            take_snapshot(src_woo, fix("w1"))
            take_snapshot(src_woo, fix("w2"))
            take_snapshot(src_woo, fix("w3"))
            for j in range(q):
                src_woo_qoo = create_filesystem(src_woo, f"qoo{j}")
                take_snapshot(src_woo_qoo, fix("q1"))
                take_snapshot(src_woo_qoo, fix("q2"))
                take_snapshot(src_woo_qoo, fix("q3"))

    def setup_basic_with_recursive_replication_done(self) -> None:
        self.setup_basic()
        self.run_bzfs(SRC_ROOT_DATASET, DST_ROOT_DATASET, "--recursive")
        self.assert_snapshots(DST_ROOT_DATASET, 3, "s")
        self.assert_snapshots(DST_ROOT_DATASET + "/foo", 3, "t")
        self.assert_snapshots(DST_ROOT_DATASET + "/foo/a", 3, "u")
        self.assertFalse(dataset_exists(DST_ROOT_DATASET + "/foo/b"))  # b/c src has no snapshots

    @staticmethod
    def log_dir_opt() -> list[str]:
        return ["--log-dir", os.path.join(get_home_directory(), argparse_cli.LOG_DIR_DEFAULT + "-test")]

    def run_bzfs_internal(
        self,
        *arguments: str,
        dry_run: bool | None = None,
        no_create_bookmark: bool = False,
        no_use_bookmark: bool = False,
        skip_on_error: str = "fail",
        retries: int = 0,
        expected_status: int | list[int] = 0,
        error_injection_triggers: dict[str, Counter] | None = None,
        delete_injection_triggers: dict[str, Counter] | None = None,
        param_injection_triggers: dict[str, dict[str, bool]] | None = None,
        inject_params: dict[str, bool] | None = None,
        max_command_line_bytes: int | None = None,
        creation_prefix: str | None = None,
        max_exceptions_to_summarize: int | None = None,
        max_datasets_per_minibatch_on_list_snaps: int | None = None,
        ssh_control_persist_margin_secs: int | None = None,
        isatty: bool | None = None,
        progress_update_intervals: tuple[float, float] | None = None,
        use_select: bool | None = None,
        use_jobrunner: bool = False,
        spawn_process_per_job: bool | None = None,
        cache_snapshots: bool = False,
        force_stable_cache_namespace: bool = False,
    ) -> bzfs.Job | bzfs_jobrunner.Job:
        port = getenv_any("test_ssh_port")  # set this if sshd is on non-standard port: export bzfs_test_ssh_port=12345
        args = list(arguments)
        src_host = [] if use_jobrunner else ["--ssh-src-host", "127.0.0.1"]
        dst_host = [] if use_jobrunner else ["--ssh-dst-host", "127.0.0.1"]
        ssh_dflt_port = "2222" if SSH_PROGRAM == "hpnssh" else "22"  # see https://www.psc.edu/hpn-ssh-home/hpn-readme/
        src_port = ["--ssh-src-port", ssh_dflt_port if port is None else str(port)]
        dst_port = [] if port is None else ["--ssh-dst-port", str(port)]
        src_user = ["--ssh-src-user", os_username()]
        params = self.param
        if params and params.get("ssh_mode") == "push":
            args += dst_host + dst_port
        elif params and params.get("ssh_mode") == "pull":
            args += src_host + src_port
        elif params and params.get("ssh_mode") == "pull-push":
            if (
                getenv_bool("test_enable_IPv6", True)
                and RNG.randint(0, 2) % 3 == 0
                and not platform.platform().startswith("FreeBSD-")
                and not SSH_PROGRAM == "hpnssh"
                and not force_stable_cache_namespace
            ):
                src_host = [] if use_jobrunner else ["--ssh-src-host", "::1"]  # IPv6 syntax for 127.0.0.1 loopback address
                dst_host = [] if use_jobrunner else ["--ssh-dst-host", "::1"]  # IPv6 syntax for 127.0.0.1 loopback address
            args += src_host + dst_host + src_port + dst_port
            if params and "min_pipe_transfer_size" in params and int(params["min_pipe_transfer_size"]) == 0:
                args += src_user
            args += ["--bwlimit=10000m"]
        elif params and params.get("ssh_mode", "local") != "local":
            raise ValueError("Unknown ssh_mode: " + params["ssh_mode"])

        args += [
            "--ssh-src-config-file=" + SSH_CONFIG_FILE,
            "--ssh-dst-config-file=" + SSH_CONFIG_FILE,
        ]

        for arg in args:
            assert "--retries" not in arg
        args += [f"--retries={retries}"]
        args += self.log_dir_opt()

        if params and "skip_missing_snapshots" in params:
            i = find_match(args, lambda arg: arg.startswith("-"))
            i = max(i, 0)
            args = args[0:i] + ["--skip-missing-snapshots=" + str(params["skip_missing_snapshots"])] + args[i:]

        if self.is_no_privilege_elevation():
            # test ZFS delegation in combination with --no-privilege-elevation flag
            args = args + ["--no-privilege-elevation"]
            src_permissions = "send,snapshot,hold,bookmark"
            if delete_injection_triggers is not None:
                src_permissions += ",destroy,mount"
            # optional_dst_permissions = ",canmount,mountpoint,readonly,compression,encryption,keylocation,recordsize"
            optional_dst_permissions = ",keylocation,compression"
            dst_permissions = "mount,create,receive,rollback,destroy" + optional_dst_permissions
            cmd = f"sudo -n zfs allow -u {os_username()} {src_permissions}".split(" ") + [SRC_POOL_NAME]
            if dataset_exists(SRC_POOL_NAME):
                run_cmd(cmd)
            cmd = f"sudo -n zfs allow -u {os_username()} {dst_permissions}".split(" ") + [DST_POOL_NAME]
            if dataset_exists(DST_POOL_NAME):
                run_cmd(cmd)

        if SSH_PROGRAM != "ssh" and "--ssh-program" not in args and "--ssh-program=" not in args:
            args = args + ["--ssh-program=" + SSH_PROGRAM]

        if params and params.get("verbose", None):
            args = args + ["--verbose"]

        if params and "min_pipe_transfer_size" in params:
            old_min_pipe_transfer_size = os.environ.get(ENV_VAR_PREFIX + "min_pipe_transfer_size")
            os.environ[ENV_VAR_PREFIX + "min_pipe_transfer_size"] = str(int(params["min_pipe_transfer_size"]))

        if dry_run:
            args = args + ["--dryrun=recv"]

        if no_create_bookmark:
            args = args + ["--create-bookmarks=none"]

        if no_use_bookmark:
            args = args + ["--no-use-bookmark"]

        if skip_on_error:
            args = args + ["--skip-on-error=" + skip_on_error]

        args = args + ["--exclude-envvar-regex=EDITOR"]
        args += ["--cache-snapshots"] if cache_snapshots else []

        job: bzfs.Job | bzfs_jobrunner.Job
        if use_jobrunner:
            job = bzfs_jobrunner.Job(log=None, termination_event=threading.Event())
            job.is_test_mode = True
            if spawn_process_per_job:
                args += ["--spawn-process-per-job"]
        else:
            job = bzfs.Job()
            job.is_test_mode = True

            if error_injection_triggers is not None:
                job.error_injection_triggers = error_injection_triggers
                args = args + ["--threads=1"]

            if delete_injection_triggers is not None:
                job.delete_injection_triggers = delete_injection_triggers
                args = args + ["--threads=1"]

            if param_injection_triggers is not None:
                job.param_injection_triggers = param_injection_triggers
                args = args + ["--threads=1"]

            if inject_params is not None:
                job.inject_params = inject_params

            if max_command_line_bytes is not None:
                job.max_command_line_bytes = max_command_line_bytes

            if creation_prefix is not None:
                job.creation_prefix = creation_prefix

            if max_exceptions_to_summarize is not None:
                job.max_exceptions_to_summarize = max_exceptions_to_summarize

            if use_select is not None:
                job.use_select = use_select

            if progress_update_intervals is not None:
                job.progress_update_intervals = progress_update_intervals

        old_ssh_control_persist_margin_secs = os.environ.get(ENV_VAR_PREFIX + "ssh_control_persist_margin_secs")
        if ssh_control_persist_margin_secs is not None:
            os.environ[ENV_VAR_PREFIX + "ssh_control_persist_margin_secs"] = str(ssh_control_persist_margin_secs)

        old_max_datasets_per_minibatch_on_list_snaps = os.environ.get(
            ENV_VAR_PREFIX + "max_datasets_per_minibatch_on_list_snaps"
        )
        if max_datasets_per_minibatch_on_list_snaps is not None:
            os.environ[ENV_VAR_PREFIX + "max_datasets_per_minibatch_on_list_snaps"] = str(
                max_datasets_per_minibatch_on_list_snaps
            )

        old_dedicated_tcp_connection_per_zfs_send = os.environ.get(ENV_VAR_PREFIX + "dedicated_tcp_connection_per_zfs_send")
        if platform.platform().startswith("FreeBSD-13"):
            # workaround for spurious hangs during zfs send/receive in ~30% of Github Action jobs on QEMU
            # probably caused by https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=283101
            # via https://github.com/openzfs/zfs/issues/16731#issuecomment-2561987688
            os.environ[ENV_VAR_PREFIX + "dedicated_tcp_connection_per_zfs_send"] = "false"

        old_isatty = os.environ.get(ENV_VAR_PREFIX + "isatty")
        if isatty is not None:
            os.environ[ENV_VAR_PREFIX + "isatty"] = str(isatty)

        returncode = 0
        try:
            if use_jobrunner:
                assert isinstance(job, bzfs_jobrunner.Job)
                job.run_main([bzfs_jobrunner.PROG_NAME] + args)
            else:
                assert isinstance(job, bzfs.Job)
                job.run_main(bzfs.argument_parser().parse_args(args), args)
        except subprocess.CalledProcessError as e:
            returncode = e.returncode
            if expected_status != returncode:
                traceback.print_exc()
        except SystemExit as e:
            assert isinstance(e.code, int)
            returncode = e.code
            if expected_status != returncode:
                traceback.print_exc()
        finally:
            if self.is_no_privilege_elevation():
                # revoke all ZFS delegation permissions
                cmd = f"sudo -n zfs unallow -r -u {os_username()}".split(" ") + [SRC_POOL_NAME]
                if dataset_exists(SRC_POOL_NAME):
                    run_cmd(cmd)
                cmd = f"sudo -n zfs unallow -r -u {os_username()}".split(" ") + [DST_POOL_NAME]
                if dataset_exists(DST_POOL_NAME):
                    run_cmd(cmd)

            if params and "min_pipe_transfer_size" in params:
                if old_min_pipe_transfer_size is None:
                    os.environ.pop(ENV_VAR_PREFIX + "min_pipe_transfer_size", None)
                else:
                    os.environ[ENV_VAR_PREFIX + "min_pipe_transfer_size"] = old_min_pipe_transfer_size

            if max_datasets_per_minibatch_on_list_snaps is not None:
                if old_max_datasets_per_minibatch_on_list_snaps is None:
                    os.environ.pop(ENV_VAR_PREFIX + "max_datasets_per_minibatch_on_list_snaps", None)
                else:
                    os.environ[ENV_VAR_PREFIX + "max_datasets_per_minibatch_on_list_snaps"] = (
                        old_max_datasets_per_minibatch_on_list_snaps
                    )

            if old_dedicated_tcp_connection_per_zfs_send is None:
                os.environ.pop(ENV_VAR_PREFIX + "dedicated_tcp_connection_per_zfs_send", None)
            else:
                os.environ[ENV_VAR_PREFIX + "dedicated_tcp_connection_per_zfs_send"] = (
                    old_dedicated_tcp_connection_per_zfs_send
                )

            if old_isatty is None:
                os.environ.pop(ENV_VAR_PREFIX + "isatty", None)
            else:
                os.environ[ENV_VAR_PREFIX + "isatty"] = old_isatty

            if old_ssh_control_persist_margin_secs is None:
                os.environ.pop(ENV_VAR_PREFIX + "ssh_control_persist_margin_secs", None)
            else:
                os.environ[ENV_VAR_PREFIX + "ssh_control_persist_margin_secs"] = old_ssh_control_persist_margin_secs

        if isinstance(expected_status, list):
            self.assertIn(returncode, expected_status)
        else:
            self.assertEqual(expected_status, returncode)
        return job

    def run_bzfs(self, *args: str, **kwargs: Any) -> bzfs.Job:
        job = self.run_bzfs_internal(*args, **kwargs)
        assert isinstance(job, bzfs.Job)
        return job

    def run_bzfs_jobrunner(self, *args: str, **kwargs: Any) -> bzfs_jobrunner.Job:
        job = self.run_bzfs_internal(*args, use_jobrunner=True, **kwargs)
        assert isinstance(job, bzfs_jobrunner.Job)
        return job

    def assert_snapshot_names(self, dataset: str, expected_names: list[str]) -> None:
        dataset = build(dataset)
        snap_names = natsorted([snapshot_name(snapshot) for snapshot in snapshots(dataset)])
        expected_names = [fix(name) for name in expected_names]
        self.assertListEqual(expected_names, snap_names)

    def assert_snapshots(
        self, dataset: str, expected_num_snapshots: int, snapshot_prefix: str = "", offset: int = 0
    ) -> None:
        expected_names = [f"{snapshot_prefix}{i + 1 + offset}" for i in range(expected_num_snapshots)]
        self.assert_snapshot_names(dataset, expected_names)

    def assert_snapshot_name_regexes(self, dataset: str, expected_names: list[str]) -> None:
        dataset = build(dataset)
        snap_names = natsorted([snapshot_name(snapshot) for snapshot in snapshots(dataset)])
        expected_names = [fix(name) for name in expected_names]
        self.assertEqual(len(expected_names), len(snap_names), f"{expected_names} vs. {snap_names}")
        for expected_name, snap_name in zip(expected_names, snap_names):
            self.assertRegex(snap_name, expected_name, f"{expected_names} vs. {snap_names}")

    def assert_bookmark_names(self, dataset: str, expected_names: list[str]) -> None:
        dataset = build(dataset)
        snap_names = natsorted([bookmark_name(bookmark) for bookmark in bookmarks(dataset)])
        expected_names = [fix(name) for name in expected_names]
        self.assertListEqual(expected_names, snap_names)

    def is_no_privilege_elevation(self) -> bool:
        return bool(self.param and self.param.get("no_privilege_elevation", False))

    def is_encryption_mode(self) -> bool:
        return bool(self.param and self.param.get("encrypted_dataset", False))

    @staticmethod
    def properties_with_special_characters() -> dict[str, str]:
        return {
            "compression": "off",
            "bzfs:prop0": "/tmp/dir with  spaces and $dollar sign-" + str(os.getpid()),
            "bzfs:prop1": "/tmp/dir` ~!@#$%^&*()_+-={}[]|;:<>?,./",  # test escaping
            "bzfs:prop2": "/tmp/foo'bar",
            "bzfs:prop3": '/tmp/foo"bar',
            "bzfs:prop4": "/tmp/foo'ba\"rbaz",
            "bzfs:prop5": '/tmp/foo"ba\'r"baz',
            "bzfs:prop6": "/tmp/foo  bar\t\t\nbaz\n\n\n",
            "bzfs:prop7": "/tmp/foo\\bar",
            "bzfs:prop8": "\\\\server\\share",  # UNC-style path with leading double backslashes
            "bzfs:prop9": "/tmp/foo\\`bar",  # backslash + backtick sequence exercises dquote edge case
        }

    def generate_recv_resume_token(self, from_snapshot: str | None, to_snapshot: str, dst_dataset: str) -> None:
        snapshot_opts = to_snapshot if not from_snapshot else f"-i {from_snapshot} {to_snapshot}"
        send = f"sudo -n zfs send --raw --compressed -v {snapshot_opts}"
        c = INJECT_DST_PIPE_FAIL_KBYTES
        cmd = ["sh", "-c", f"{send} | dd bs=1024 count={c} 2>/dev/null | sudo -n zfs receive -v -F -u -s {dst_dataset}"]
        try:
            subprocess.run(cmd, stdout=PIPE, stderr=PIPE, text=True, check=True)
        except subprocess.CalledProcessError as e:
            logging.getLogger(__name__).warning(f"generate_recv_resume_token stdout: {e.stdout}")
            self.assertIn("Partially received snapshot is saved", e.stderr)
        receive_resume_token = dataset_property(dst_dataset, "receive_resume_token")
        self.assertTrue(receive_resume_token)
        self.assertNotEqual("-", receive_resume_token)
        self.assertNotIn(dst_dataset + to_snapshot[to_snapshot.index("@") :], snapshots(dst_dataset))

    def assert_receive_resume_token(self, dataset: str, exists: bool) -> str:
        receive_resume_token = dataset_property(dataset, "receive_resume_token")
        if exists:
            self.assertTrue(receive_resume_token)
            self.assertNotEqual("-", receive_resume_token)
        else:
            self.assertEqual("-", receive_resume_token)
        return receive_resume_token


def create_filesystems(path: str, props: list[str] | None = None) -> str:
    create_filesystem(SRC_ROOT_DATASET, path, props=props)
    return create_filesystem(DST_ROOT_DATASET, path, props=props)


def recreate_filesystem(dataset: str, props: list[str] | None = None) -> str:
    if dataset_exists(dataset):
        destroy(dataset, recursive=True)
    return create_filesystem(dataset, props=props)


def create_volumes(path: str, props: list[str] | None = None) -> str:
    create_volume(SRC_ROOT_DATASET, path, size="1M", props=props)
    return create_volume(DST_ROOT_DATASET, path, size="1M", props=props)


def detect_zpool_features(location: str, pool: str) -> None:
    assert ZPOOL_FEATURES is not None
    cmd = "zpool get -Hp -o property,value all".split(" ") + [pool]
    lines = run_cmd(cmd)
    props = {line.split("\t", 1)[0]: line.split("\t", 1)[1] for line in lines}
    features = {k: v for k, v in props.items() if k.startswith("feature@")}
    ZPOOL_FEATURES[location] = features


def is_zpool_feature_enabled_or_active(location: str, feature: str) -> bool:
    assert ZPOOL_FEATURES is not None
    return ZPOOL_FEATURES[location].get(feature, "") in ("active", "enabled")


def are_bookmarks_enabled(location: str) -> bool:
    return is_zpool_feature_enabled_or_active(location, "feature@bookmark_v2") and is_zpool_feature_enabled_or_active(
        location, "feature@bookmark_written"
    )


def is_zpool_recv_resume_feature_enabled_or_active() -> bool:
    return is_zpool_feature_enabled_or_active("src", "feature@extensible_dataset") and is_zfs_at_least_2_1_0()


def is_cache_snapshots_enabled() -> bool:
    return is_zfs_at_least_2_2_0()


def fix(s: str) -> str:
    """Generate names containing leading and trailing whitespace, forbidden characters, etc."""
    return AFIX + s + AFIX


def natsorted(
    iterable: Iterable[str],
    key: Callable[[str], str] | None = None,
    reverse: bool = False,
) -> list[str]:
    """Returns a new list containing all items from the iterable in ascending order.

    If `key` is specified, it will be used to extract a comparison key from each list element.
    """
    if key is None:
        return sorted(iterable, key=natsort_key, reverse=reverse)
    else:
        return sorted(iterable, key=lambda x: natsort_key(key(x)), reverse=reverse)


def natsort_key(s: str) -> tuple[str, int, str]:
    """Sorts strings that may contain non-negative integers according to numerical value if any two strings have the same
    non-numeric prefix.

    Example: s1 < s3 < s10 < s10a < s10b
    """
    match = re.fullmatch(r"(\D*)(\d*)(.*)", s)
    if match:
        prefix, num, suffix = match.groups()
        num = int(num) if num else 0
        return prefix, num, suffix
    return s, 0, ""


def is_zfs_at_least_2_3_0() -> bool:
    ver = zfs_version()
    if ver is None:
        return False
    return is_version_at_least(ver, "2.3.0")


def is_zfs_at_least_2_2_0() -> bool:
    ver = zfs_version()
    if ver is None:
        return False
    return is_version_at_least(ver, "2.2.0")


def is_zfs_at_least_2_1_0() -> bool:
    ver = zfs_version()
    if ver is None:
        return False
    return is_version_at_least(ver, "2.1.0")


def is_pv_at_least_1_9_0() -> bool:
    ver = pv_version()
    return is_version_at_least(ver, "1.9.0")


def pv_version() -> str:
    """Example pv 1.8.5 -> 1.8.5."""
    lines = subprocess.run(["pv", "--version"], capture_output=True, text=True, check=True).stdout
    assert lines
    assert lines.startswith("pv")
    line = lines.splitlines()[0]
    version = line.split(" ")[1].strip()
    match = re.fullmatch(r"(\d+\.\d+\.\d+).*", version)
    if match:
        return match.group(1)
    else:
        raise ValueError("Unparsable pv version string: " + version)


def os_username() -> str:
    # return getpass.getuser()
    return pwd.getpwuid(os.getuid()).pw_name
