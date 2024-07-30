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

import argparse
import getpass
import itertools
import logging
import platform
import traceback
import unittest
import os
import sys
import tempfile
from collections import defaultdict, Counter
from unittest.mock import patch, mock_open
from zfs_util import *
sys.path.append(os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'wbackup_zfs')))
import wbackup_zfs
from wbackup_zfs import CheckRange

src_pool_name = 'wb_src'
dst_pool_name = 'wb_dst'
pool_size = 100 * 1024 * 1024
die_status = 3
prog_exe = './wbackup_zfs/wbackup_zfs.py'
zpool_features = None
afix = ""
encryption_algo = 'aes-256-gcm'
qq = "wbackup_zfs_"
zfs_encryption_key_fd, zfs_encryption_key = tempfile.mkstemp(prefix='test_wbackup_zfs.key_')
os.write(zfs_encryption_key_fd, 'mypasswd'.encode())
os.close(zfs_encryption_key_fd)
keylocation = f"file://{zfs_encryption_key}"
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(message)s',
                    # datefmt='%Y-%m-%d %H:%M:%S',
                    datefmt='%H:%M:%S:',
                    )
error_injection_triggers = 'error_injection_triggers'


def getenv_any(key, default=None):
    return os.getenv(qq + key, default)


def getenv_bool(key, default=False):
    return getenv_any(key, str(default).lower()).strip().lower() == "true"


sudo_cmd = []
if getenv_bool('test_enable_sudo', True) and os.geteuid() != 0:
    sudo_cmd = ['sudo']
    set_sudo_cmd(['sudo'])


def fix(str):
    """Generate names containing leading and trailing whitespace, forbidden characters, etc."""
    return afix + str + afix


#############################################################################
class ParametrizedTestCase(unittest.TestCase):
    """ TestCase classes that want to be parametrized should
        inherit from this class.
    """
    def __init__(self, methodName='runTest', param=None):
        super(ParametrizedTestCase, self).__init__(methodName)
        self.param = param

    @staticmethod
    def parametrize(testcase_klass, param=None):
        """ Create a suite containing all test taken from the given
            subclass, passing them the parameter 'param'.
        """
        testloader = unittest.TestLoader()
        testnames = testloader.getTestCaseNames(testcase_klass)
        suite = unittest.TestSuite()
        for name in testnames:
            suite.addTest(testcase_klass(name, param=param))
        return suite


#############################################################################
class WBackupTestCase(ParametrizedTestCase):

    def setUp(self):
        global src_pool, dst_pool
        global src_root_dataset, dst_root_dataset
        global afix

        for pool in src_pool_name, dst_pool_name:
            if dataset_exists(pool):
                destroy_pool(pool)
            if not dataset_exists(pool):
                tmp = tempfile.NamedTemporaryFile()
                tmp.seek(pool_size - 1)
                tmp.write(b'0')
                tmp.seek(0)
                run_cmd(sudo_cmd + ['zpool', 'create', pool, tmp.name])

        src_pool = build(src_pool_name)
        dst_pool = build(dst_pool_name)
        afix = self.param.get('affix', '')
        src_root_dataset = recreate_dataset(src_pool_name + '/tmp/' + fix('src'))
        dst_root_dataset = recreate_dataset(dst_pool_name + '/tmp/' + fix('dst'))

        global zpool_features
        if zpool_features is None:
            zpool_features = {}
            detect_zpool_features('src', src_pool_name)
            print(f"zpool bookmarks feature: {is_zpool_bookmarks_feature_enabled_or_active('src')}", file=sys.stderr)
            props = zpool_features['src']
            features = '\n'.join([f"{k}: {v}" for k, v in sorted(props.items()) if k.startswith('feature@')])
            print(f"zpool features: {features}", file=sys.stderr)

    # zpool list -o name|grep '^wb_'|xargs -n 1 -r --verbose zpool destroy; rm -fr /tmp/tmp* /run/user/$UID/wbackup-zfs/
    def tearDown(self):
        pass
        # for pool in [src_pool_name, dst_pool_name]:
        #     destroy_pool(pool)

    def tearDownAndSetup(self):
        self.tearDown()
        self.setUp()

    def setup_basic(self):
        compression_props = ['-o', 'compression=on']
        encryption_props = ['-o', f"encryption={encryption_algo}"]
        if is_solaris_zfs():
            encryption_props += ['-o', f"keysource=passphrase,{keylocation}"]
        else:
            encryption_props += ['-o', 'keyformat=passphrase', '-o', f"keylocation={keylocation}"]

        dataset_props = encryption_props + compression_props if self.is_encryption_mode() else compression_props
        src_foo = create_dataset(src_root_dataset, 'foo', props=dataset_props)
        src_foo_a = create_dataset(src_foo, 'a')
        src_foo_b = create_dataset(src_foo, 'b')
        take_snapshot(src_root_dataset, fix('s1'))
        take_snapshot(src_root_dataset, fix('s2'))
        take_snapshot(src_root_dataset, fix('s3'))
        take_snapshot(src_foo, fix('t1'))
        take_snapshot(src_foo, fix('t2'))
        take_snapshot(src_foo, fix('t3'))
        take_snapshot(src_foo_a, fix('u1'))
        take_snapshot(src_foo_a, fix('u2'))
        take_snapshot(src_foo_a, fix('u3'))

    def setup_basic_with_recursive_replication_done(self):
        self.setup_basic()
        self.run_wbackup(src_root_dataset, dst_root_dataset, '--recursive')
        self.assertSnapshots(dst_root_dataset, 3, 's')
        self.assertSnapshots(dst_root_dataset + "/foo", 3, 't')
        self.assertSnapshots(dst_root_dataset + "/foo/a", 3, 'u')
        self.assertFalse(dataset_exists(dst_root_dataset + '/foo/b'))  # b/c src has no snapshots

    def run_wbackup(self, *args, dry_run=None, no_create_bookmark=False, no_use_bookmark=False, expected_status=0):
        port = getenv_any('test_ssh_port')  # set this if sshd is on a non-standard port: export wbackup_zfs_test_ssh_port=12345
        args = list(args)
        src_host = ['--ssh-src-host', '127.0.0.1']
        dst_host = ['--ssh-dst-host', '127.0.0.1']
        src_port = [] if port is None else ['--ssh-src-port', str(port)]
        dst_port = [] if port is None else ['--ssh-dst-port', str(port)]
        params = self.param
        if params and params.get('ssh_mode') == 'push':
            args = args + dst_host + dst_port
        elif params and params.get('ssh_mode') == 'pull':
            args = args + src_host + src_port
        elif params and params.get('ssh_mode') == 'pull-push':
            args = args + src_host + dst_host + src_port + dst_port
        elif params and params.get('ssh_mode', 'local') != 'local':
            raise ValueError("Unknown ssh_mode: " + params['ssh_mode'])

        if params and params.get('ssh_mode', 'local') != 'local':
            args = args + ['--ssh-src-extra-opt', '-o StrictHostKeyChecking=no',
                           '--ssh-dst-extra-opt', '-o StrictHostKeyChecking=no']

        if params and 'skip_missing_snapshots' in params:
            args = args + ['--skip-missing-snapshots=' + str(params['skip_missing_snapshots'])]

        if self.is_no_privilege_elevation():
            # test ZFS delegation in combination with --no-privilege-elevation flag
            args = args + ['--no-privilege-elevation']
            src_permissions = 'send'
            if not is_solaris_zfs():
                src_permissions += ',bookmark'
            optional_dst_permissions = ',canmount,mountpoint,readonly,compression,encryption,keylocation,recordsize'
            optional_dst_permissions = ',keylocation,compression' if not is_solaris_zfs() else ',keysource,encryption,salt,compression,checksum'
            dst_permissions = 'mount,create,receive,rollback,destroy' + optional_dst_permissions
            cmd = f"sudo zfs allow -u {getpass.getuser()} {src_permissions}".split(' ') + [src_pool_name]
            if dataset_exists(src_pool_name):
                run_cmd(cmd)
            cmd = f"sudo zfs allow -u {getpass.getuser()} {dst_permissions}".split(' ') + [dst_pool_name]
            if dataset_exists(dst_pool_name):
                run_cmd(cmd)

        if params and params.get('verbose', None):
            args = args + ['--verbose']

        if params and 'min_transfer_size' in params:
            old_min_transfer_size = os.environ.get(qq + 'min_transfer_size')
            os.environ[qq + 'min_transfer_size'] = str(int(params['min_transfer_size']))

        if dry_run:
            args = args + ['--dry-run']

        if no_create_bookmark:
            args = args + ['--no-create-bookmark']

        if no_use_bookmark:
            args = args + ['--no-use-bookmark']

        args = args + ['--bwlimit=10000m']

        job = wbackup_zfs.Job()
        job.is_test_mode = True
        if params and error_injection_triggers in params:
            job.error_injection_triggers = params[error_injection_triggers]

        returncode = 0
        try:
            # returncode = subprocess.run([prog_exe] + args).returncode
            job.run_main(wbackup_zfs.argument_parser().parse_args(args), args)
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
                cmd = f"sudo zfs unallow -r -u {getpass.getuser()}".split(' ') + [src_pool_name]
                if dataset_exists(src_pool_name):
                    run_cmd(cmd)
                cmd = f"sudo zfs unallow -r -u {getpass.getuser()}".split(' ') + [dst_pool_name]
                if dataset_exists(dst_pool_name):
                    run_cmd(cmd)

            if params and 'min_transfer_size' in params:
                if old_min_transfer_size is None:
                    os.environ.pop(qq + 'min_transfer_size')
                else:
                    os.environ[qq + 'min_transfer_size'] = old_min_transfer_size

        self.assertEqual(expected_status, returncode)
        return job

    def assertSnapshotNames(self, dataset, expected_names):
        dataset = build(dataset)
        snap_names = natsorted([snapshot_name(snapshot) for snapshot in snapshots(dataset)])
        expected_names = [fix(name) for name in expected_names]
        self.assertListEqual(expected_names, snap_names)

    def assertSnapshots(self, dataset, expected_num_snapshots, snapshot_prefix='', offset=0):
        expected_names = [f"{snapshot_prefix}{i+1+offset}" for i in range(0, expected_num_snapshots)]
        self.assertSnapshotNames(dataset, expected_names)

    def assertBookmarkNames(self, dataset, expected_names):
        dataset = build(dataset)
        snap_names = natsorted([bookmark_name(bookmark) for bookmark in bookmarks(dataset)])
        expected_names = [fix(name) for name in expected_names]
        self.assertListEqual(expected_names, snap_names)

    def is_no_privilege_elevation(self):
        return self.param and self.param.get('no_privilege_elevation', False)

    def is_encryption_mode(self):
        return self.param and self.param.get('encrypted_dataset', False)


#############################################################################
class LocalTestCase(WBackupTestCase):

    def test_basic_replication_flat_nothing_todo(self):
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset, dst_root_dataset, dry_run=(i == 0))
            self.assertSnapshots(dst_root_dataset, 0)
            self.assertFalse(dataset_exists(dst_root_dataset + '/foo'))

    def test_basic_replication_without_source(self):
        destroy(src_root_dataset, recursive=True)
        recreate_dataset(dst_root_dataset)
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset, dst_root_dataset, dry_run=(i == 0), expected_status=die_status)
            self.assertTrue(dataset_exists(dst_root_dataset))
            self.assertSnapshots(dst_root_dataset, 0)

    def test_basic_replication_flat_simple(self):
        self.setup_basic()
        for i in range(0, 3):
            if i <= 1:
                job = self.run_wbackup(src_root_dataset, dst_root_dataset, dry_run=(i == 0))
            else:
                job = self.run_wbackup(src_root_dataset, dst_root_dataset, '--quiet', dry_run=(i == 0))
            self.assertFalse(dataset_exists(dst_root_dataset + '/foo'))
            if i == 0:
                self.assertSnapshots(dst_root_dataset, 0)
            else:
                self.assertSnapshots(dst_root_dataset, 3, 's')
            for loc in ['local', 'src', 'dst']:
                if loc != 'local':
                    self.assertTrue(job.is_program_available('zfs', loc))
                self.assertTrue(job.is_program_available('zpool', loc))
                self.assertTrue(job.is_program_available('ssh', loc))
                self.assertTrue(job.is_program_available('sh', loc))
                self.assertTrue(job.is_program_available('sudo', loc))
                self.assertTrue(job.is_program_available('zstd', loc))
                self.assertTrue(job.is_program_available('mbuffer', loc))
                self.assertTrue(job.is_program_available('pv', loc))

    def test_basic_replication_flat_nonzero_snapshots_create_parents(self):
        self.assertFalse(dataset_exists(dst_root_dataset + '/foo'))
        self.assertFalse(dataset_exists(dst_root_dataset + '/foo/a'))
        self.setup_basic()
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset + '/foo/a', dst_root_dataset + '/foo/a', dry_run=(i == 0))
            self.assertFalse(dataset_exists(dst_root_dataset + '/foo/b'))
            if i == 0:
                self.assertFalse(dataset_exists(dst_root_dataset + '/foo/a'))
            else:
                self.assertSnapshots(dst_root_dataset + '/foo/a', 3, 'u')

    def test_basic_replication_flat_no_snapshot_dont_create_parents(self):
        self.assertFalse(dataset_exists(dst_root_dataset + '/foo/b'))
        self.setup_basic()
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset + '/foo/b', dst_root_dataset + '/foo/b', dry_run=(i == 0))
            self.assertFalse(dataset_exists(dst_root_dataset + '/foo'))

    def test_basic_replication_recursive1(self):
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertFalse(dataset_exists(dst_root_dataset + '/foo'))
        self.setup_basic()
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset, dst_root_dataset, '--recursive', dry_run=(i == 0))
            if i == 0:
                self.assertSnapshots(dst_root_dataset, 0)
                self.assertFalse(dataset_exists(dst_root_dataset + '/foo'))
            else:
                self.assertSnapshots(dst_root_dataset, 3, 's')
                self.assertSnapshots(dst_root_dataset + "/foo", 3, 't')
                self.assertSnapshots(dst_root_dataset + "/foo/a", 3, 'u')
                self.assertFalse(dataset_exists(dst_root_dataset + '/foo/b'))  # b/c src has no snapshots

                compression_prop = dataset_property(dst_root_dataset + "/foo", 'compression')
                self.assertEqual(compression_prop, 'on')
                encryption_prop = dataset_property(dst_root_dataset, 'encryption')
                self.assertEqual(encryption_prop, 'off')
                encryption_prop = dataset_property(dst_root_dataset + "/foo", 'encryption')
                self.assertEqual(encryption_prop, encryption_algo if self.is_encryption_mode() else 'off')
                encryption_prop = dataset_property(dst_root_dataset + "/foo/a", 'encryption')
                self.assertEqual(encryption_prop, encryption_algo if self.is_encryption_mode() else 'off')

    def test_basic_replication_recursive_with_exclude_dataset(self):
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertFalse(dataset_exists(dst_root_dataset + '/foo'))
        self.setup_basic()
        boo = create_dataset(src_root_dataset, 'goo')
        take_snapshot(boo, fix('g1'))
        boo = create_dataset(src_root_dataset, 'boo')
        take_snapshot(boo, fix('b1'))
        zoo = create_dataset(src_root_dataset, 'zoo')
        take_snapshot(zoo, fix('z1'))
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset, dst_root_dataset, '--recursive',
                             '--include-dataset=',
                             '--exclude-dataset=/' + src_root_dataset + '/foo',
                             '--include-dataset=/' + src_root_dataset + '/foo',
                             '--exclude-dataset=/' + dst_root_dataset + '/goo/',
                             '--include-dataset=/' + dst_root_dataset + '/goo',
                             '--include-dataset=/xxxxxxxxx',
                             '--exclude-dataset=boo/',
                             '--include-dataset=boo',
                             dry_run=(i == 0))
            self.assertFalse(dataset_exists(dst_root_dataset + '/foo'))
            self.assertFalse(dataset_exists(dst_root_dataset + '/goo'))
            self.assertFalse(dataset_exists(dst_root_dataset + '/boo'))
            if i == 0:
                self.assertSnapshots(dst_root_dataset, 0)
            else:
                self.assertSnapshots(dst_root_dataset, 3, 's')
                self.assertSnapshots(dst_root_dataset + "/zoo", 1, 'z')

    def test_basic_replication_flat_simple_with_sufficiently_many_retries_on_error_injection(self):
        self.basic_replication_flat_simple_with_retries_on_error_injection(max_retries=6, expected_status=0)

    def test_basic_replication_flat_simple_with_insufficiently_many_retries_on_error_injection(self):
        self.basic_replication_flat_simple_with_retries_on_error_injection(max_retries=5, expected_status=1)

    def basic_replication_flat_simple_with_retries_on_error_injection(self, max_retries=0, expected_status=0):
        self.setup_basic()
        create_dataset(dst_root_dataset)
        params = self.param
        old_triggers = params.get(error_injection_triggers)

        # inject failures for this many tries. only after that finally succeed the operation
        counter = Counter(zfs_list_snapshot_dst=2, full_zfs_send=2, incremental_zfs_send=2)

        params[error_injection_triggers] = counter
        try:
            self.run_wbackup(src_root_dataset, dst_root_dataset, f"--max-retries={max_retries}",
                             expected_status=expected_status)
            self.assertEqual(0, counter['zfs_list_snapshot_dst'])  # i.e, it took 2-0=2 retries to succeed
            self.assertEqual(0, counter['full_zfs_send'])
            self.assertEqual(0, counter['incremental_zfs_send'])
            if expected_status == 0:
                self.assertSnapshots(dst_root_dataset, 3, 's')
        finally:
            if params:
                if old_triggers:
                    params[error_injection_triggers] = old_triggers
                else:
                    params.pop(error_injection_triggers, None)

    def test_basic_replication_flat_with_bookmarks1(self):
        if not is_zpool_bookmarks_feature_enabled_or_active('src'):
            self.skipTest("ZFS has no bookmark feature")
        take_snapshot(src_root_dataset, fix('d1'))
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset, dst_root_dataset, dry_run=(i == 0))
            if i == 0:
                self.assertSnapshotNames(src_root_dataset, ['d1'])
                self.assertBookmarkNames(src_root_dataset, [])
            else:
                self.assertSnapshotNames(dst_root_dataset, ['d1'])
                self.assertBookmarkNames(src_root_dataset, ['d1'])

        # delete snapshot, which will cause no problem as we still have its bookmark
        destroy(snapshots(src_root_dataset)[0])
        self.assertSnapshotNames(src_root_dataset, [])
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset, dst_root_dataset, '--skip-missing-snapshots=error', dry_run=(i == 0))
            self.assertSnapshotNames(dst_root_dataset, ['d1'])  # nothing has changed
            self.assertBookmarkNames(src_root_dataset, ['d1'])  # nothing has changed

        # take another snapshot and replicate it without problems as we still have the bookmark
        take_snapshot(src_root_dataset, fix('d2'))
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset, dst_root_dataset, '--skip-missing-snapshots=error', dry_run=(i == 0))
            if i == 0:
                self.assertSnapshotNames(dst_root_dataset, ['d1'])  # nothing has changed
                self.assertBookmarkNames(src_root_dataset, ['d1'])  # nothing has changed
            else:
                self.assertSnapshotNames(dst_root_dataset, ['d1', 'd2'])
                self.assertBookmarkNames(src_root_dataset, ['d1', 'd2'])

    def test_basic_replication_flat_with_bookmarks2(self):
        if not is_zpool_bookmarks_feature_enabled_or_active('src'):
            self.skipTest("ZFS has no bookmark feature")
        take_snapshot(src_root_dataset, fix('d1'))
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset, dst_root_dataset, dry_run=(i == 0))
            if i == 0:
                self.assertSnapshotNames(src_root_dataset, ['d1'])
                self.assertBookmarkNames(src_root_dataset, [])
            else:
                self.assertSnapshotNames(dst_root_dataset, ['d1'])
                self.assertBookmarkNames(src_root_dataset, ['d1'])

        # rename snapshot, which will cause no problem as we still have its bookmark
        cmd = sudo_cmd + ['zfs', 'rename', snapshots(src_root_dataset)[0], snapshots(src_root_dataset)[0]+'h']
        run_cmd(cmd)

        for i in range(0, 2):
            # replicate while excluding the rename snapshot
            self.run_wbackup(src_root_dataset, dst_root_dataset, '--exclude-snapshot-regex=.*h', '--skip-missing-snapshots=error', dry_run=(i == 0))
            self.assertSnapshotNames(dst_root_dataset, ['d1'])  # nothing has changed
            self.assertBookmarkNames(src_root_dataset, ['d1'])  # nothing has changed

        # take another snapshot and replicate it without problems as we still have the bookmark
        take_snapshot(src_root_dataset, fix('d2'))
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset, dst_root_dataset, '--exclude-snapshot-regex=.*h', '--skip-missing-snapshots=error', dry_run=(i == 0))
            if i == 0:
                self.assertSnapshotNames(dst_root_dataset, ['d1'])  # nothing has changed
                self.assertBookmarkNames(src_root_dataset, ['d1'])  # nothing has changed
            else:
                self.assertSnapshotNames(dst_root_dataset, ['d1', 'd2'])
                self.assertBookmarkNames(src_root_dataset, ['d1', 'd2'])

    def test_basic_replication_flat_with_bookmarks3(self):
        if not is_zpool_bookmarks_feature_enabled_or_active('src'):
            self.skipTest("ZFS has no bookmark feature")
        take_snapshot(src_root_dataset, fix('d1'))
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset, dst_root_dataset, '--no-create-bookmark', dry_run=(i == 0))
            if i == 0:
                self.assertSnapshotNames(src_root_dataset, ['d1'])
                self.assertBookmarkNames(src_root_dataset, [])
            else:
                self.assertSnapshotNames(dst_root_dataset, ['d1'])
                self.assertBookmarkNames(src_root_dataset, [])
        snapshot_tag = snapshots(src_root_dataset)[0].split('@', 1)[1]
        create_bookmark(src_root_dataset, snapshot_tag, snapshot_tag+'h')
        create_bookmark(src_root_dataset, snapshot_tag, snapshot_tag)
        self.assertBookmarkNames(src_root_dataset, ['d1', 'd1h'])

        # delete snapshot, which will cause no problem as we still have its bookmark
        destroy(snapshots(src_root_dataset)[0])
        self.assertSnapshotNames(src_root_dataset, [])

        for i in range(1, 2):
            # replicate while excluding hourly snapshots
            self.run_wbackup(src_root_dataset, dst_root_dataset, '--exclude-snapshot-regex=.*h', '--skip-missing-snapshots=error', dry_run=(i == 0))
            self.assertSnapshotNames(dst_root_dataset, ['d1'])  # nothing has changed
            self.assertBookmarkNames(src_root_dataset, ['d1', 'd1h'])  # nothing has changed

        # take another snapshot and replicate it without problems as we still have the bookmark
        take_snapshot(src_root_dataset, fix('d2'))
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset, dst_root_dataset, '--exclude-snapshot-regex=.*h', '--skip-missing-snapshots=error', dry_run=(i == 0))
            if i == 0:
                self.assertSnapshotNames(dst_root_dataset, ['d1'])  # nothing has changed
                self.assertBookmarkNames(src_root_dataset, ['d1', 'd1h'])  # nothing has changed
            else:
                self.assertSnapshotNames(dst_root_dataset, ['d1', 'd2'])
                self.assertBookmarkNames(src_root_dataset, ['d1', 'd1h', 'd2'])

    def test_complex_replication_flat_with_no_create_bookmark(self):
        self.assertFalse(dataset_exists(dst_root_dataset + '/foo'))
        self.setup_basic()
        src_foo = build(src_root_dataset + '/foo')
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0), no_create_bookmark=True)
            self.assertSnapshots(dst_root_dataset, 0)
            if i == 0:
                self.assertFalse(dataset_exists(dst_root_dataset + '/foo'))
            else:
                self.assertSnapshots(dst_root_dataset + '/foo', 3, 't')
                self.assertFalse(dataset_exists(dst_root_dataset + '/foo/a'))
                self.assertFalse(dataset_exists(dst_root_dataset + '/foo/b'))

        # on src take some snapshots
        take_snapshot(src_foo, fix('t4'))
        take_snapshot(src_foo, fix('t5'))
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0), no_create_bookmark=True)
            self.assertSnapshots(dst_root_dataset, 0)
            if i == 0:
                self.assertSnapshots(dst_root_dataset + '/foo', 3, 't')
                self.assertFalse(dataset_exists(dst_root_dataset + '/foo/a'))
                self.assertFalse(dataset_exists(dst_root_dataset + '/foo/b'))
            else:
                self.assertSnapshots(dst_root_dataset + '/foo', 5, 't')

        # on src take another snapshot
        take_snapshot(src_foo, fix('t6'))
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0), no_create_bookmark=True)
            self.assertSnapshots(dst_root_dataset, 0)
            if i == 0:
                self.assertSnapshots(dst_root_dataset + '/foo', 5, 't')
            else:
                self.assertSnapshots(dst_root_dataset + '/foo', 6, 't')

        # on dst (rather than src) take some snapshots, which is asking for trouble...
        dst_foo = build(dst_root_dataset + '/foo')
        take_snapshot(dst_foo, fix('t7'))
        take_snapshot(dst_foo, fix('t8'))
        # Conflict: Most recent destination snapshot is more recent than most recent common snapshot
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0), no_create_bookmark=True, expected_status=die_status)
            self.assertSnapshots(dst_root_dataset + '/foo', 8, 't')  # nothing has changed on dst

        # resolve conflict via dst rollback to most recent common snapshot
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', '--force-once', dry_run=(i == 0))
            self.assertSnapshots(dst_root_dataset, 0)
            if i == 0:
                self.assertSnapshots(dst_root_dataset + '/foo', 8, 't')  # nothing has changed on dst
            else:
                self.assertSnapshots(dst_root_dataset + '/foo', 6, 't')

        # on src and dst, take some snapshots, which is asking for trouble again...
        src_guid = snapshot_property(take_snapshot(src_foo, fix('t7')), 'guid')
        dst_guid = snapshot_property(take_snapshot(dst_foo, fix('t7')), 'guid')
        # names of t7 are the same but GUIDs are different as they are not replicas of each other - t7 is not a common snapshot.
        self.assertNotEqual(src_guid, dst_guid)
        take_snapshot(dst_foo, fix('t8'))
        # Conflict: Most recent destination snapshot is more recent than most recent common snapshot
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0), no_create_bookmark=True, expected_status=die_status)
            self.assertSnapshots(dst_root_dataset + '/foo', 8, 't')  # nothing has changed on dst
            self.assertEqual(dst_guid, snapshot_property(snapshots(build(dst_root_dataset + '/foo'))[6], 'guid'))  # nothing has changed on dst

        # resolve conflict via dst rollback to most recent common snapshot prior to replicating
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', '--force-once', dry_run=(i == 0), no_create_bookmark=True)  # resolve conflict via dst rollback
            self.assertSnapshots(dst_root_dataset, 0)
            if i == 0:
                self.assertSnapshots(dst_root_dataset + '/foo', 8, 't')  # nothing has changed on dst
                self.assertEqual(dst_guid, snapshot_property(snapshots(build(dst_root_dataset + '/foo'))[6], 'guid'))  # nothing has changed on dst
            else:
                self.assertSnapshots(dst_root_dataset + '/foo', 7, 't')
                self.assertEqual(src_guid, snapshot_property(snapshots(build(dst_root_dataset + '/foo'))[6], 'guid'))  # now they are true replicas

        # on src delete some snapshots that are older than most recent common snapshot, which is normal and won't cause changes to dst
        destroy(snapshots(src_foo)[0])
        destroy(snapshots(src_foo)[2])
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0), no_create_bookmark=True)
            self.assertSnapshots(dst_root_dataset, 0)
            self.assertSnapshots(dst_root_dataset + '/foo', 7, 't')

        # replicate a child dataset
        self.run_wbackup(src_root_dataset + '/foo/a', dst_root_dataset + '/foo/a', no_create_bookmark=True)
        self.assertSnapshots(dst_root_dataset + '/foo/a', 3, 'u')
        self.assertSnapshots(dst_root_dataset, 0)
        self.assertSnapshots(dst_root_dataset + '/foo', 7, 't')

        # on src delete all snapshots so now there is no common snapshot anymore, which is trouble...
        for snap in snapshots(src_foo):
            destroy(snap)
        take_snapshot(src_foo, fix('t9'))
        take_snapshot(src_foo, fix('t10'))
        take_snapshot(src_foo, fix('t11'))
        self.assertSnapshots(src_root_dataset + '/foo', 3, 't', offset=8)
        # Conflict: no common snapshot was found.
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0), no_create_bookmark=True, expected_status=die_status)
            self.assertSnapshots(dst_root_dataset + '/foo', 7, 't')  # nothing has changed on dst
            self.assertSnapshots(dst_root_dataset + '/foo/a', 3, 'u')

        # resolve conflict via deleting all dst snapshots prior to replication
        for i in range(0, 3):
            if i > 0 and self.is_encryption_mode():
                # potential workaround?: rerun once with -R --skip-missing added to zfs_send_program_opts
                self.skipTest("zfs receive -F cannot be used to destroy an encrypted filesystem - https://github.com/openzfs/zfs/issues/6793")
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', '--f1', dry_run=(i == 0), no_create_bookmark=True)
            self.assertSnapshots(dst_root_dataset, 0)
            self.assertSnapshots(dst_root_dataset + '/foo/a', 3, 'u')
            if i == 0:
                self.assertSnapshots(dst_root_dataset + '/foo', 7, 't')  # nothing changed
            else:
                self.assertSnapshots(dst_root_dataset + '/foo', 3, 't', offset=8)

        # no change on src means replication is a noop:
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0), no_create_bookmark=True)
            self.assertSnapshots(dst_root_dataset, 0)
            self.assertSnapshots(dst_root_dataset + '/foo', 3, 't', offset=8)

        # no change on src means replication is a noop:
        for i in range(0, 2):
            # rollback dst to most recent snapshot
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', '--force', dry_run=(i == 0), no_create_bookmark=True)
            self.assertSnapshots(dst_root_dataset, 0)
            self.assertSnapshots(dst_root_dataset + '/foo', 3, 't', offset=8)

    def test_complex_replication_flat_use_bookmarks(self):
        if not is_zpool_bookmarks_feature_enabled_or_active('src'):
            self.skipTest("ZFS has no bookmark feature")
        self.assertFalse(dataset_exists(dst_root_dataset + '/foo'))
        self.setup_basic()
        src_foo = build(src_root_dataset + '/foo')
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0))
            self.assertSnapshots(dst_root_dataset, 0)
            if i == 0:
                self.assertFalse(dataset_exists(dst_root_dataset + '/foo'))
                self.assertBookmarkNames(src_root_dataset + '/foo', [])
            else:
                self.assertSnapshots(dst_root_dataset + '/foo', 3, 't')
                self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3'])
                self.assertFalse(dataset_exists(dst_root_dataset + '/foo/a'))
                self.assertFalse(dataset_exists(dst_root_dataset + '/foo/b'))

        # on src take some snapshots
        take_snapshot(src_foo, fix('t4'))
        take_snapshot(src_foo, fix('t5'))
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0))
            self.assertSnapshots(dst_root_dataset, 0)
            if i == 0:
                self.assertSnapshots(dst_root_dataset + '/foo', 3, 't')
                self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3'])
                self.assertFalse(dataset_exists(dst_root_dataset + '/foo/a'))
                self.assertFalse(dataset_exists(dst_root_dataset + '/foo/b'))
            else:
                self.assertSnapshots(dst_root_dataset + '/foo', 5, 't')
                self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5'])

        # on src take another snapshot
        take_snapshot(src_foo, fix('t6'))
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0))
            self.assertSnapshots(dst_root_dataset, 0)
            if i == 0:
                self.assertSnapshots(dst_root_dataset + '/foo', 5, 't')
                self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5'])
            else:
                self.assertSnapshots(dst_root_dataset + '/foo', 6, 't')
                self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5', 't6'])

        # on dst (rather than src) take some snapshots, which is asking for trouble...
        dst_foo = build(dst_root_dataset + '/foo')
        take_snapshot(dst_foo, fix('t7'))
        take_snapshot(dst_foo, fix('t8'))
        # Conflict: Most recent destination snapshot is more recent than most recent common snapshot
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0), expected_status=die_status)
            self.assertSnapshots(dst_root_dataset + '/foo', 8, 't')  # nothing has changed on dst
            self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5', 't6'])

        # resolve conflict via dst rollback to most recent common snapshot
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', '--force-once', dry_run=(i == 0))
            self.assertSnapshots(dst_root_dataset, 0)
            if i == 0:
                self.assertSnapshots(dst_root_dataset + '/foo', 8, 't')  # nothing has changed on dst
                self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5', 't6'])
            else:
                self.assertSnapshots(dst_root_dataset + '/foo', 6, 't')
                self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5', 't6'])

        # on src and dst, take some snapshots, which is asking for trouble again...
        src_guid = snapshot_property(take_snapshot(src_foo, fix('t7')), 'guid')
        dst_guid = snapshot_property(take_snapshot(dst_foo, fix('t7')), 'guid')
        # names of t7 are the same but GUIDs are different as they are not replicas of each other - t7 is not a common snapshot.
        self.assertNotEqual(src_guid, dst_guid)
        take_snapshot(dst_foo, fix('t8'))
        # Conflict: Most recent destination snapshot is more recent than most recent common snapshot
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0), expected_status=die_status)
            self.assertSnapshots(dst_root_dataset + '/foo', 8, 't')  # nothing has changed on dst
            self.assertEqual(dst_guid, snapshot_property(snapshots(build(dst_root_dataset + '/foo'))[6], 'guid'))  # nothing has changed on dst
            self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5', 't6'])

        # resolve conflict via dst rollback to most recent common snapshot prior to replicating
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', '--force-once', dry_run=(i == 0))  # resolve conflict via dst rollback
            self.assertSnapshots(dst_root_dataset, 0)
            if i == 0:
                self.assertSnapshots(dst_root_dataset + '/foo', 8, 't')  # nothing has changed on dst
                self.assertEqual(dst_guid, snapshot_property(snapshots(build(dst_root_dataset + '/foo'))[6], 'guid'))  # nothing has changed on dst
                self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5', 't6'])
            else:
                self.assertSnapshots(dst_root_dataset + '/foo', 7, 't')
                self.assertEqual(src_guid, snapshot_property(snapshots(build(dst_root_dataset + '/foo'))[6], 'guid'))  # now they are true replicas
                self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5', 't6', 't7'])

        # on src delete some snapshots that are older than most recent common snapshot, which is normal and won't cause changes to dst
        destroy(snapshots(src_foo)[0])
        destroy(snapshots(src_foo)[2])
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0))
            self.assertSnapshots(dst_root_dataset, 0)
            self.assertSnapshots(dst_root_dataset + '/foo', 7, 't')
            self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5', 't6', 't7'])

        # replicate a child dataset
        self.run_wbackup(src_root_dataset + '/foo/a', dst_root_dataset + '/foo/a')
        self.assertSnapshots(dst_root_dataset + '/foo/a', 3, 'u')
        self.assertBookmarkNames(src_root_dataset + '/foo/a', ['u1', 'u3'])
        self.assertSnapshots(dst_root_dataset + '/foo', 7, 't')

        # on src delete all snapshots so now there is no common snapshot anymore,
        # which isn't actually trouble because we have bookmarks for them...
        for snap in snapshots(src_foo):
            destroy(snap)
        # No Conflict: no common snapshot was found, but we found a (common) bookmark that can be used instead
        # so replication is a noop and won't fail:
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0))
            self.assertSnapshots(dst_root_dataset + '/foo', 7, 't')  # nothing has changed on dst
            self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5', 't6', 't7'])
            self.assertSnapshots(dst_root_dataset + '/foo/a', 3, 'u')

        take_snapshot(src_foo, fix('t9'))
        take_snapshot(src_foo, fix('t10'))
        take_snapshot(src_foo, fix('t11'))
        self.assertSnapshots(src_root_dataset + '/foo', 3, 't', offset=8)

        # No Conflict: no common snapshot was found, but we found a (common) bookmark that can be used instead
        # so replication will succeed:
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0))
            self.assertSnapshots(dst_root_dataset + '/foo/a', 3, 'u')
            if i == 0:
                self.assertSnapshots(dst_root_dataset + '/foo', 7, 't')  # nothing has changed on dst
                self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5', 't6', 't7'])
            else:
                self.assertSnapshotNames(dst_root_dataset + '/foo',
                                     ['t1', 't2', 't3', 't4', 't5', 't6', 't7', 't9', 't10', 't11'])
                self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5', 't6', 't7', 't11'])

        # no change on src means replication is a noop:
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', '--force', dry_run=(i == 0))
            self.assertSnapshotNames(dst_root_dataset + '/foo',
                                     ['t1', 't2', 't3', 't4', 't5', 't6', 't7', 't9', 't10', 't11'])
            self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5', 't6', 't7', 't11'])

        # on src delete the most recent snapshot and its bookmark, which is trouble as now src has nothing
        # in common anymore with the most recent dst snapshot:
        destroy(natsorted(snapshots(src_foo), key=lambda s: s)[-1])  # destroy t11
        destroy(natsorted(bookmarks(src_foo), key=lambda b: b)[-1])  # destroy t11
        self.assertSnapshotNames(src_root_dataset + '/foo', ['t9', 't10'])
        self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5', 't6', 't7'])
        for i in range(0, 2):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', dry_run=(i == 0), expected_status=die_status)
            self.assertSnapshotNames(dst_root_dataset + '/foo',
                                     ['t1', 't2', 't3', 't4', 't5', 't6', 't7', 't9', 't10', 't11'])  # nothing has changed
            self.assertBookmarkNames(src_root_dataset + '/foo', ['t1', 't3', 't5', 't6', 't7'])  # nothing has changed

        # resolve conflict via dst rollback to most recent common snapshot prior to replicating
        take_snapshot(src_foo, fix('t12'))
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset + '/foo', dst_root_dataset + '/foo', '--force-once', dry_run=(i == 0))
            self.assertSnapshots(dst_root_dataset + '/foo/a', 3, 'u')
            if i == 0:
                self.assertSnapshotNames(dst_root_dataset + '/foo',
                                         ['t1', 't2', 't3', 't4', 't5', 't6', 't7', 't9', 't10', 't11'])  # nothing has changed
                self.assertBookmarkNames(src_root_dataset + '/foo',
                                         ['t1', 't3', 't5', 't6', 't7'])  # nothing has changed
            else:
                self.assertSnapshotNames(dst_root_dataset + '/foo',
                                         ['t1', 't2', 't3', 't4', 't5', 't6', 't7', 't9', 't10', 't12'])  # nothing has changed
                self.assertBookmarkNames(src_root_dataset + '/foo',
                                         ['t1', 't3', 't5', 't6', 't7', 't12'])

    def test_nostream1(self):
        self.setup_basic()
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset, dst_root_dataset, '--no-stream', dry_run=(i == 0))
            if i == 0:
                self.assertSnapshots(dst_root_dataset, 0)
            else:
                self.assertSnapshotNames(dst_root_dataset, ['s3'])

        take_snapshot(src_root_dataset, fix('s4'))
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset, dst_root_dataset, '--no-stream', dry_run=(i == 0))
            if i == 0:
                self.assertSnapshotNames(dst_root_dataset, ['s3'])
            else:
                self.assertSnapshotNames(dst_root_dataset, ['s3', 's4'])

        take_snapshot(src_root_dataset, fix('s5'))
        take_snapshot(src_root_dataset, fix('s6'))
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset, dst_root_dataset, '--no-stream', dry_run=(i == 0))
            if i == 0:
                self.assertSnapshotNames(dst_root_dataset, ['s3', 's4'])
            else:
                self.assertSnapshotNames(dst_root_dataset, ['s3', 's4', 's6'])

    def test_basic_replication_flat_pool(self):
        for child in datasets(src_pool) + datasets(dst_pool):
            destroy(child, recursive=True)
        for snapshot in snapshots(src_pool) + snapshots(dst_pool):
            destroy(snapshot)
        for i in range(0, 2):
            self.run_wbackup(src_pool, dst_pool, dry_run=(i == 0))
            self.assertSnapshots(dst_pool, 0, 'p')  # nothing has changed

        take_snapshot(src_pool, fix('p1'))
        for i in range(0, 2):
            if i > 0 and self.is_no_privilege_elevation():  # maybe related: https://github.com/openzfs/zfs/issues/10461
                self.skipTest(f"'cannot unmount '/wb_dst': permission denied' error on zfs receive -F -u wb_dst")
            self.run_wbackup(src_pool, dst_pool, dry_run=(i == 0))
            if i == 0:
                self.assertSnapshots(dst_pool, 0, 'p')  # nothing has changed
            else:
                self.assertSnapshots(dst_pool, 1, 'p')

        for snapshot in snapshots(dst_pool):
            destroy(snapshot)
        take_snapshot(dst_pool, fix('q1'))
        for i in range(0, 3):
            self.run_wbackup(src_pool, dst_pool, '--force-once', dry_run=(i == 0))
            if i == 0:
                self.assertSnapshots(dst_pool, 1, 'q')
            else:
                self.assertSnapshots(dst_pool, 1, 'p')

    def test_basic_replication_missing_pools(self):
        for child in datasets(src_pool) + datasets(dst_pool):
            destroy(child, recursive=True)
        for snapshot in snapshots(src_pool) + snapshots(dst_pool):
            destroy(snapshot)
        for i in range(0, 2):
            self.run_wbackup(src_pool, dst_pool, dry_run=(i == 0))
            self.assertSnapshots(dst_pool, 0)  # nothing has changed

        destroy_pool(dst_pool)
        for i in range(0, 2):
            self.run_wbackup(src_pool, dst_pool, dry_run=(i == 0), expected_status=die_status)
            self.assertFalse(dataset_exists(dst_pool))

        destroy_pool(src_pool)
        for i in range(0, 2):
            self.run_wbackup(src_pool, dst_pool, dry_run=(i == 0), expected_status=die_status)
            self.assertFalse(dataset_exists(src_pool))

    def test_basic_replication_dataset_with_spaces(self):
        d1 = ' foo  zoo  '
        src_foo = create_dataset(src_root_dataset, d1)
        s1 = fix(' s  nap1   ')
        take_snapshot(src_foo, fix(s1))
        d2 = '..::   exit HOME f1.2 echo '
        src_foo_a = create_dataset(src_foo, d2)
        t1 = fix(d2 + 'snap')
        take_snapshot(src_foo_a, fix(t1))
        self.run_wbackup(src_root_dataset, dst_root_dataset, '--recursive')
        self.assertTrue(dataset_exists(dst_root_dataset + '/' + d1))
        self.assertSnapshotNames(dst_root_dataset + '/' + d1, [s1])
        self.assertTrue(dataset_exists(dst_root_dataset + '/' + d1 + '/' + d2))
        self.assertSnapshotNames(dst_root_dataset + '/' + d1 + '/' + d2, [t1])

    def test_delete_missing_datasets_with_missing_src_root(self):
        destroy(src_root_dataset, recursive=True)
        recreate_dataset(dst_root_dataset)
        for i in range(0, 3):
            self.run_wbackup(src_root_dataset, dst_root_dataset,
                             '--skip-replication', '--delete-missing-datasets', dry_run=(i == 0))
            if i == 0:
                self.assertTrue(dataset_exists(dst_root_dataset))
            else:
                self.assertFalse(dataset_exists(dst_root_dataset))

    def test_delete_missing_datasets_flat_nothing_todo(self):
        self.setup_basic_with_recursive_replication_done()
        take_snapshot(create_dataset(dst_root_dataset, 'bar'), 'b1')
        destroy(build(src_root_dataset + '/foo'), recursive=True)
        self.assertFalse(dataset_exists(src_root_dataset + '/foo'))
        self.assertTrue(dataset_exists(src_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + '/foo'))
        self.run_wbackup(src_root_dataset, dst_root_dataset, '--skip-replication', '--delete-missing-datasets')
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + '/foo'))
        self.assertTrue(dataset_exists(dst_root_dataset + '/bar'))

    def test_delete_missing_datasets_recursive1(self):
        self.setup_basic_with_recursive_replication_done()
        take_snapshot(create_dataset(dst_root_dataset, 'bar'), fix('b1'))
        take_snapshot(create_dataset(dst_root_dataset, 'zoo'), fix('z1'))
        destroy(build(src_root_dataset + '/foo'), recursive=True)
        self.assertFalse(dataset_exists(src_root_dataset + '/foo'))
        self.assertTrue(dataset_exists(src_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + '/foo'))
        self.run_wbackup(src_root_dataset, dst_root_dataset, '--recursive',
                         '--skip-replication', '--delete-missing-datasets')
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertFalse(dataset_exists(dst_root_dataset + '/foo'))
        self.assertFalse(dataset_exists(dst_root_dataset + '/bar'))
        self.assertFalse(dataset_exists(dst_root_dataset + '/zoo'))

    def test_delete_missing_datasets_with_exclude_regex1(self):
        self.setup_basic_with_recursive_replication_done()
        take_snapshot(create_dataset(dst_root_dataset, 'bar'), fix('b1'))
        take_snapshot(create_dataset(dst_root_dataset, 'zoo'), fix('z1'))
        destroy(build(src_root_dataset + '/foo'), recursive=True)
        self.assertFalse(dataset_exists(src_root_dataset + '/foo'))
        self.assertTrue(dataset_exists(src_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + '/foo'))
        self.run_wbackup(src_root_dataset, dst_root_dataset, '--recursive',
                         '--skip-replication', '--delete-missing-datasets',
                         '--exclude-dataset-regex', 'bar?', '--exclude-dataset-regex', 'zoo*')
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertFalse(dataset_exists(dst_root_dataset + '/foo'))
        self.assertTrue(dataset_exists(dst_root_dataset + '/bar'))
        self.assertTrue(dataset_exists(dst_root_dataset + '/zoo'))

    def test_delete_missing_datasets_with_exclude_regex2(self):
        self.setup_basic_with_recursive_replication_done()
        take_snapshot(create_dataset(dst_root_dataset, 'bar'), fix('b1'))
        take_snapshot(create_dataset(dst_root_dataset, 'zoo'), fix('z1'))
        destroy(build(src_root_dataset + '/foo'), recursive=True)
        self.assertFalse(dataset_exists(src_root_dataset + '/foo'))
        self.assertTrue(dataset_exists(src_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + '/foo'))
        self.run_wbackup(src_root_dataset, dst_root_dataset, '--recursive',
                         '--skip-replication', '--delete-missing-datasets',
                         '--exclude-dataset-regex', '!bar')
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + '/foo'))
        self.assertFalse(dataset_exists(dst_root_dataset + '/bar'))
        self.assertTrue(dataset_exists(dst_root_dataset + '/zoo'))

    def test_delete_missing_datasets_with_exclude_dataset(self):
        self.setup_basic_with_recursive_replication_done()
        take_snapshot(create_dataset(dst_root_dataset, 'bar'), fix('b1'))
        take_snapshot(create_dataset(dst_root_dataset, 'zoo'), fix('z1'))
        destroy(build(src_root_dataset + '/foo'), recursive=True)
        self.assertFalse(dataset_exists(src_root_dataset + '/foo'))
        self.assertTrue(dataset_exists(src_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + '/foo'))
        self.run_wbackup(src_root_dataset, dst_root_dataset, '--recursive',
                         '--skip-replication', '--delete-missing-datasets',
                         '--exclude-dataset', 'foo',
                         '--exclude-dataset', 'zoo',
                         '--exclude-dataset', 'foo/b',
                         '--exclude-dataset', 'xxxxx',
                         )
        self.assertTrue(dataset_exists(dst_root_dataset))
        self.assertTrue(dataset_exists(dst_root_dataset + '/foo'))
        self.assertTrue(dataset_exists(dst_root_dataset + '/foo/a'))
        self.assertFalse(dataset_exists(dst_root_dataset + '/foo/b'))
        self.assertFalse(dataset_exists(dst_root_dataset + '/bar'))
        self.assertTrue(dataset_exists(dst_root_dataset + '/zoo'))

    def test_delete_missing_datasets_and_empty_datasets(self):
        create_datasets('axe')
        create_datasets('foo/a')
        create_datasets('foo/a/b')
        create_datasets('foo/a/b/c')
        create_datasets('foo/a/b/d')
        take_snapshot(create_datasets('foo/a/e'), fix('e1'))
        create_datasets('foo/b/c')
        create_datasets('foo/b/c/d')
        create_datasets('foo/b/d')
        take_snapshot(create_datasets('foo/c'), fix('c1'))
        create_datasets('zoo')
        create_datasets('boo')
        self.run_wbackup(src_root_dataset, dst_root_dataset, '--recursive',
                         '--skip-replication', '--delete-missing-datasets', '--exclude-dataset', 'boo')
        self.assertFalse(dataset_exists(dst_root_dataset + '/axe'))
        self.assertTrue(dataset_exists(dst_root_dataset + '/foo/a'))
        self.assertFalse(dataset_exists(dst_root_dataset + '/foo/a/b'))
        self.assertTrue(dataset_exists(dst_root_dataset + '/foo/a/e'))
        self.assertFalse(dataset_exists(dst_root_dataset + '/foo/b'))
        self.assertTrue(dataset_exists(dst_root_dataset + '/foo/c'))
        self.assertFalse(dataset_exists(dst_root_dataset + '/zoo'))
        self.assertTrue(dataset_exists(dst_root_dataset + '/boo'))

    def test_delete_missing_snapshots_nothing_todo(self):
        self.setup_basic_with_recursive_replication_done()
        self.assertTrue(dataset_exists(src_root_dataset + '/foo/b'))
        self.assertEqual(0, len(snapshots(build(src_root_dataset + '/foo/b'))))
        self.assertFalse(dataset_exists(dst_root_dataset + '/foo/b'))
        self.run_wbackup(src_root_dataset + '/foo/b', dst_root_dataset + '/foo/b',
                         '--skip-replication', '--delete-missing-snapshots')
        self.assertFalse(dataset_exists(dst_root_dataset + '/foo/b'))

    def test_delete_missing_snapshots_flat(self):
        self.setup_basic_with_recursive_replication_done()
        destroy(snapshots(src_root_dataset)[2])
        destroy(snapshots(src_root_dataset)[0])
        src_foo = build(src_root_dataset + '/foo')
        destroy(snapshots(src_foo)[1])
        src_foo_a = build(src_root_dataset + '/foo/a')
        destroy(snapshots(src_foo_a)[2])
        self.run_wbackup(src_root_dataset, dst_root_dataset, '--skip-replication', '--delete-missing-snapshots')
        self.assertSnapshotNames(dst_root_dataset, ['s2'])
        self.assertSnapshots(dst_root_dataset + "/foo", 3, 't')
        self.assertSnapshots(dst_root_dataset + "/foo/a", 3, 'u')

    def test_delete_missing_snapshots_despite_same_name(self):
        self.setup_basic_with_recursive_replication_done()
        destroy(snapshots(src_root_dataset)[2])
        destroy(snapshots(src_root_dataset)[0])
        take_snapshot(src_root_dataset, fix('s1'))  # Note: not the same as prior snapshot (has different GUID)
        take_snapshot(src_root_dataset, fix('s3'))  # Note: not the same as prior snapshot (has different GUID)
        self.run_wbackup(src_root_dataset, dst_root_dataset, '--skip-replication', '--delete-missing-snapshots')
        self.assertSnapshotNames(dst_root_dataset, ['s2'])

    def test_delete_missing_snapshots_recursive(self):
        self.setup_basic_with_recursive_replication_done()
        destroy(snapshots(src_root_dataset)[2])
        destroy(snapshots(src_root_dataset)[0])
        src_foo = build(src_root_dataset + '/foo')
        destroy(snapshots(src_foo)[1])
        src_foo_a = build(src_root_dataset + '/foo/a')
        destroy(snapshots(src_foo_a)[2])
        self.run_wbackup(src_root_dataset, dst_root_dataset, '--recursive',
                         '--skip-replication', '--delete-missing-snapshots')
        self.assertSnapshotNames(dst_root_dataset, ['s2'])
        self.assertSnapshotNames(dst_root_dataset + '/foo', ['t1', 't3'])
        self.assertSnapshotNames(dst_root_dataset + '/foo/a', ['u1', 'u2'])

    def test_delete_missing_snapshots_with_excludes_flat_nothing_todo(self):
        self.setup_basic_with_recursive_replication_done()
        self.run_wbackup(src_root_dataset, dst_root_dataset, '--skip-replication', '--delete-missing-snapshots',
                         '--exclude-snapshot-regex', 'xxxx*')
        self.assertSnapshots(dst_root_dataset, 3, 's')
        self.assertSnapshots(dst_root_dataset + "/foo", 3, 't')
        self.assertSnapshots(dst_root_dataset + "/foo/a", 3, 'u')

    def test_delete_missing_snapshots_with_excludes_flat(self):
        self.setup_basic_with_recursive_replication_done()
        for snap in snapshots(src_root_dataset):
            destroy(snap)
        for snap in snapshots(build(src_root_dataset + '/foo')):
            destroy(snap)
        self.run_wbackup(src_root_dataset, dst_root_dataset, '--skip-replication', '--delete-missing-snapshots',
                         '--exclude-snapshot-regex', r'!.*s[1-2]+.*')
        self.assertSnapshotNames(dst_root_dataset, ['s3'])
        self.assertSnapshots(dst_root_dataset + "/foo", 3, 't')
        self.assertSnapshots(dst_root_dataset + "/foo/a", 3, 'u')

    def test_delete_missing_snapshots_with_excludes_recursive(self):
        self.setup_basic_with_recursive_replication_done()
        for snap in snapshots(src_root_dataset):
            destroy(snap)
        for snap in snapshots(build(src_root_dataset + '/foo')):
            destroy(snap)
        self.run_wbackup(src_root_dataset, dst_root_dataset, '--recursive',
                         '--skip-replication', '--delete-missing-snapshots',
                         '--exclude-snapshot-regex', '.*s[1-2]+.*',
                         '--exclude-snapshot-regex', '.*t1.*',
                         '--exclude-snapshot-regex', '.*u.*')
        self.assertSnapshotNames(dst_root_dataset, ['s1', 's2'])
        self.assertSnapshotNames(dst_root_dataset + '/foo', ['t1'])
        self.assertSnapshotNames(dst_root_dataset + '/foo/a', ['u1', 'u2', 'u3'])

    def test_delete_missing_snapshots_with_excludes_recursive_and_excluding_dataset_regex(self):
        self.setup_basic_with_recursive_replication_done()
        for snap in snapshots(src_root_dataset):
            destroy(snap)
        for snap in snapshots(build(src_root_dataset + '/foo')):
            destroy(snap)
        self.run_wbackup(src_root_dataset, dst_root_dataset, '--recursive',
                         '--skip-replication', '--delete-missing-snapshots',
                         '--exclude-dataset-regex', 'foo',
                         '--exclude-snapshot-regex', '.*s[1-2]+.*',
                         '--exclude-snapshot-regex', '.*t1.*',
                         '--exclude-snapshot-regex', '.*u1.*')
        self.assertSnapshotNames(dst_root_dataset, ['s1', 's2'])
        self.assertSnapshots(dst_root_dataset + "/foo", 3, 't')
        self.assertSnapshots(dst_root_dataset + "/foo/a", 3, 'u')


#############################################################################
class RemoteTestCase(WBackupTestCase):

    def test_basic_replication_flat_nothing_todo(self):
        LocalTestCase(param=self.param).test_basic_replication_flat_nothing_todo()

    def test_basic_replication_without_source(self):
        LocalTestCase(param=self.param).test_basic_replication_without_source()

    def test_basic_replication_flat_simple(self):
        LocalTestCase(param=self.param).test_basic_replication_flat_simple()

    def test_basic_replication_recursive1(self):
        LocalTestCase(param=self.param).test_basic_replication_recursive1()

    def test_complex_replication_flat_use_bookmarks(self):
        LocalTestCase(param=self.param).test_complex_replication_flat_use_bookmarks()

    def test_basic_replication_dataset_with_spaces(self):
        LocalTestCase(param=self.param).test_basic_replication_dataset_with_spaces()

    def test_inject_src_pipe_fail(self):
        self.inject_pipe_error('inject_src_pipe_fail')

    def test_inject_dst_pipe_fail(self):
        self.inject_pipe_error('inject_dst_pipe_fail')

    def test_inject_src_pipe_garble(self):
        self.inject_pipe_error('inject_src_pipe_garble')

    def test_inject_dst_pipe_garble(self):
        self.inject_pipe_error('inject_dst_pipe_garble')

    def test_inject_src_send_error(self):
        self.inject_pipe_error('inject_src_send_error')

    def test_inject_dst_receive_error(self):
        self.inject_pipe_error('inject_dst_receive_error', expected_error=2)

    def inject_pipe_error(self, flag, expected_error=1):
        self.setup_basic()
        for i in range(0, 2):
            if i == 0:
                os.environ[qq + flag] = 'true'
            else:
                os.environ.pop(qq + flag)
            self.run_wbackup(src_root_dataset, dst_root_dataset, expected_status=expected_error if i == 0 else 0)
            if i == 0:
                self.assertSnapshots(dst_root_dataset, 0)
            else:
                self.assertSnapshots(dst_root_dataset, 3, 's')

    def test_inject_unavailable_mbuffer(self):
        self.inject_unavailable_program('inject_unavailable_mbuffer')
        if self.param and self.param.get('ssh_mode') != 'local' and self.param.get('min_transfer_size', -1) == 0:
            self.tearDownAndSetup()
            self.inject_unavailable_program('inject_failing_mbuffer', expected_error=1)

    def test_inject_unavailable_pv(self):
        self.inject_unavailable_program('inject_unavailable_pv')

    def test_inject_unavailable_sh(self):
        self.inject_unavailable_program('inject_unavailable_sh')
        self.tearDownAndSetup()
        self.inject_unavailable_program('inject_failing_sh')

    def test_inject_unavailable_zstd(self):
        self.inject_unavailable_program('inject_unavailable_zstd')

    def test_inject_unavailable_zpool(self):
        self.inject_unavailable_program('inject_unavailable_zpool')
        self.tearDownAndSetup()
        self.inject_unavailable_program('inject_failing_zpool')

    def test_inject_unavailable_ssh(self):
        if self.param and self.param.get('ssh_mode') != 'local':
            self.inject_unavailable_program('inject_unavailable_ssh', expected_error=die_status)
            self.tearDownAndSetup()
            self.inject_unavailable_program('inject_failing_ssh', expected_error=die_status)

    def test_inject_unavailable_sudo(self):
        expected_error = die_status if os.geteuid() != 0 and not self.is_no_privilege_elevation() else 0
        self.inject_unavailable_program('inject_unavailable_sudo', expected_error=expected_error)
        self.tearDownAndSetup()
        expected_error = 1 if os.geteuid() != 0 and not self.is_no_privilege_elevation() else 0
        self.inject_unavailable_program('inject_failing_sudo', expected_error=expected_error)

    def test_inject_unavailable_zfs(self):
        self.inject_unavailable_program('inject_unavailable_zfs', expected_error=die_status)
        self.tearDownAndSetup()
        self.inject_unavailable_program('inject_failing_zfs', expected_error=die_status)

    def test_disabled_sh(self):
        self.inject_unavailable_program('disable_sh')

    def test_disabled_compression(self):
        self.inject_unavailable_program('disable_compression')

    def inject_unavailable_program(self, *flags, expected_error=0):
        self.setup_basic()
        for flag in flags:
            os.environ[qq + flag] = 'true'
        self.run_wbackup(src_root_dataset, dst_root_dataset, expected_status=expected_error)
        for flag in flags:
            os.environ.pop(qq + flag)
        if expected_error != 0:
            self.assertSnapshots(dst_root_dataset, 0)
        else:
            self.assertSnapshots(dst_root_dataset, 3, 's')


#############################################################################
class IsolatedTestCase(WBackupTestCase):

    # def test_complex_replication_flat_use_bookmarks(self):
    #     LocalTestCase(param=self.param).test_complex_replication_flat_use_bookmarks()
    #
    # def test_complex_replication_flat_with_no_create_bookmark(self):
    #     LocalTestCase(param=self.param).test_complex_replication_flat_with_no_create_bookmark()

    def test_basic_replication_flat_nonzero_snapshots_create_parents(self):
        LocalTestCase(param=self.param).test_basic_replication_flat_nonzero_snapshots_create_parents()

    def test_basic_replication_flat_nonzero_snapshots_create_parents(self):
        LocalTestCase(param=self.param).test_basic_replication_flat_nonzero_snapshots_create_parents()


#############################################################################
class ExcludeSnapshotRegexTestCase(WBackupTestCase):

    def test_snapshot_series_excluding_hourlies(self):
        testcase = {}
        testcase[None] = ['d1', 'h1', 'd2', 'd3', 'd4']
        expected_results = ['d1', 'd2', 'd3', 'd4']
        dst_foo = dst_root_dataset + '/foo'

        src_foo = create_dataset(src_root_dataset, 'foo')
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        self.run_wbackup(src_foo, dst_foo,
                         '--include-snapshot-regex', 'd.*', '--exclude-snapshot-regex', 'h.*')
        self.assertSnapshotNames(dst_foo, expected_results)

        self.tearDownAndSetup()
        src_foo = create_dataset(src_root_dataset, 'foo')
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        src_snapshot = f"{src_foo}@{expected_results[0]}"
        cmd = f"sudo zfs send {src_snapshot} | sudo zfs receive -F -u {dst_foo}"  # full zfs send
        subprocess.run(cmd, text=True, check=True, shell=True)
        self.assertSnapshotNames(dst_foo, [expected_results[0]])
        self.run_wbackup(src_foo, dst_foo,
                         '--include-snapshot-regex', 'd.*', '--exclude-snapshot-regex', 'h.*')
        self.assertSnapshotNames(dst_foo, expected_results)

        self.tearDownAndSetup()
        src_foo = create_dataset(src_root_dataset, 'foo')
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        src_snapshot = f"{src_foo}@{expected_results[0]}"
        cmd = f"sudo zfs send {src_snapshot} | sudo zfs receive -F -u {dst_foo}"  # full zfs send
        subprocess.run(cmd, text=True, check=True, shell=True)
        self.assertSnapshotNames(dst_foo, [expected_results[0]])
        if is_zpool_bookmarks_feature_enabled_or_active('src'):
            create_bookmark(src_foo, expected_results[0], expected_results[0])
            destroy(src_snapshot)
            self.run_wbackup(src_foo, dst_foo,
                             '--include-snapshot-regex', 'd.*', '--exclude-snapshot-regex', 'h.*')
            self.assertSnapshotNames(dst_foo, expected_results)
            src_snapshot2 = f"{src_foo}@{expected_results[-1]}"
            destroy(src_snapshot2)  # no problem because bookmark still exists
            take_snapshot(src_foo, 'd99')
            self.run_wbackup(src_foo, dst_foo,
                             '--include-snapshot-regex', 'd.*', '--exclude-snapshot-regex', 'h.*')
            self.assertSnapshotNames(dst_foo, expected_results + ['d99'])

        self.tearDownAndSetup()
        src_foo = create_dataset(src_root_dataset, 'foo')
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        src_snapshot = f"{src_foo}@{expected_results[1]}"  # Note: [1]
        cmd = f"sudo zfs send {src_snapshot} | sudo zfs receive -F -u {dst_foo}"  # full zfs send
        subprocess.run(cmd, text=True, check=True, shell=True)
        self.assertSnapshotNames(dst_foo, [expected_results[1]])
        self.run_wbackup(src_foo, dst_foo, '--skip-missing-snapshots=false',
                         '--include-snapshot-regex', 'd.*', '--exclude-snapshot-regex', 'h.*')
        self.assertSnapshotNames(dst_foo, expected_results[1:])

        self.tearDownAndSetup()
        src_foo = create_dataset(src_root_dataset, 'foo')
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        src_snapshot = f"{src_foo}@{expected_results[1]}"  # Note: [1]
        cmd = f"sudo zfs send {src_snapshot} | sudo zfs receive -F -u {dst_foo}"  # full zfs send
        subprocess.run(cmd, text=True, check=True, shell=True)
        self.assertSnapshotNames(dst_foo, [expected_results[1]])
        self.run_wbackup(src_foo, dst_foo, '--force', '--skip-missing-snapshots=false',
                         '--exclude-snapshot-regex', '.*')
        self.assertSnapshotNames(dst_foo, [])

    def test_snapshot_series_excluding_hourlies_with_permutations(self):
        for testcase in ExcludeSnapshotRegexValidationCase().permute_snapshot_series(5):
            self.tearDownAndSetup()
            src_foo = create_dataset(src_root_dataset, 'foo')
            dst_foo = dst_root_dataset + '/foo'
            for snapshot in testcase[None]:
                take_snapshot(src_foo, snapshot)
            expected_results = testcase['d']
            # logging.info(f"input   : {','.join(testcase[None])}")
            # logging.info(f"expected: {','.join(expected_results)}")
            for i in range(0, 2):
                self.run_wbackup(src_foo, dst_foo, '--skip-missing-snapshots=false',
                                 '--include-snapshot-regex', 'd.*', '--exclude-snapshot-regex', 'h.*',
                                 dry_run=(i == 0))
                if i == 0:
                    self.assertFalse(dataset_exists(dst_foo))
                else:
                    if len(expected_results) > 0:
                        self.assertSnapshotNames(dst_foo, expected_results)
                    else:
                        self.assertFalse(dataset_exists(dst_foo))


#############################################################################
class ExcludeSnapshotRegexValidationCase(unittest.TestCase):

    def test_basic1(self):
        input_snapshots = ['d1', 'h1', 'd2', 'd3', 'd4']
        expected_results = ['d1', 'd2', 'd3', 'd4']
        self.validate_incremental_replication_steps(input_snapshots, expected_results)

    def test_basic2(self):
        input_snapshots = ['d1', 'd2', 'h1', 'd3', 'd4']
        expected_results = ['d1', 'd2', 'd3', 'd4']
        self.validate_incremental_replication_steps(input_snapshots, expected_results)

    def test_basic3(self):
        input_snapshots = ['h0', 'h1', 'd1', 'd2', 'h2', 'd3', 'd4']
        expected_results = ['d1', 'd2', 'd3', 'd4']
        self.validate_incremental_replication_steps(input_snapshots, expected_results)

    def test_basic4(self):
        input_snapshots = ['d1']
        expected_results = ['d1']
        self.validate_incremental_replication_steps(input_snapshots, expected_results)

    def test_basic5(self):
        input_snapshots = []
        expected_results = []
        self.validate_incremental_replication_steps(input_snapshots, expected_results)

    def test_validate_snapshot_series_excluding_hourlies_with_permutations(self):
        for testcase in self.permute_snapshot_series():
            self.validate_incremental_replication_steps(testcase[None], testcase['d'])

    def permute_snapshot_series(self, max_length=9):
        """
        Simulates a series of hourly and daily snapshots. At the end, makes a backup while excluding hourly
        snapshots from replication. The expectation is that after repliction dst contains all daily snapshots
        and no hourly snapshots.
        Example snapshot series: d1, h1, d2, d3, d4 --> expected dst output: d1, d2, d3, d4
        where
        d1 = first daily snapshot,  dN = n-th daily snapshot
        h1 = first hourly snapshot, hN = n-th hourly snapshot

        We test all possible permutations of series of length L=[0...max_length] snapshots
        """
        assert max_length >= 0
        testcases = []
        for L in range(0, max_length+1):
            for N in range(0, L+1):
                steps = 'd' * N + 'h' * (L-N)
                # compute a permutation of several 'd' and 'h' chars that represents the snapshot series
                for permutation in sorted(set(itertools.permutations(steps, len(steps)))):
                    snaps = defaultdict(list)
                    count = defaultdict(int)
                    for char in permutation:
                        count[char] += 1  # tag snapshots with a monotonically increasing number within each category
                        char_count = f"{count[char]:01}" if max_length < 10 else f"{count[char]:02}"  # zero pad number
                        snapshot = f"{char}{char_count}"
                        snaps[None].append(snapshot)
                        snaps[char].append(snapshot)  # represents expected results for test verification
                    testcases.append(snaps)
        return testcases

    def validate_incremental_replication_steps(self, input_snapshots, expected_results):
        # src_dataset = "s@"
        src_dataset = ""
        for force_convert_I_to_i in [False, True]:
            steps = self.incremental_replication_steps1(
                input_snapshots, src_dataset=src_dataset, force_convert_I_to_i=force_convert_I_to_i)
            # print(f"input_snapshots:" + ','.join(input_snapshots))
            # print("steps: " + ','.join([self.replication_step_to_str(step) for step in steps]))
            output_snapshots = [] if len(expected_results) == 0 else [expected_results[0]]
            output_snapshots += self.apply_incremental_replication_steps(steps, input_snapshots)
            # print(f"output_snapshots:" + ','.join(output_snapshots))
            self.assertListEqual(expected_results, output_snapshots)

    def replication_step_to_str(self, step):
        # return str(step)
        return str(step[1]) + ('-' if step[0] == '-I' else ':') + str(step[2])

    def apply_incremental_replication_steps(self, steps, input_snapshots):
        output_snapshots = []
        for i, (incr_flag, start_snapshot, end_snapshot) in enumerate(steps):
            start = input_snapshots.index(start_snapshot)
            end = input_snapshots.index(end_snapshot)
            if incr_flag == '-I':
                for j in range(start+1, end+1):
                    output_snapshots.append(input_snapshots[j])
            else:
                output_snapshots.append(input_snapshots[end])
        return output_snapshots

    def incremental_replication_steps1(self, input_snapshots, src_dataset=None, force_convert_I_to_i=False):
        origin_src_snapshots_with_guids = []
        guid = 1
        for i, snapshot in enumerate(input_snapshots):
            origin_src_snapshots_with_guids.append(f"{guid}\t{src_dataset}{snapshot}")
            guid += 1
        return self.incremental_replication_steps2(origin_src_snapshots_with_guids,
                                                   force_convert_I_to_i=force_convert_I_to_i)

    def incremental_replication_steps2(self, origin_src_snapshots_with_guids, force_convert_I_to_i=False):
        guids = []
        input_snapshots = []
        permutation = []
        included_guids = set()
        for line in origin_src_snapshots_with_guids:
            guid, snapshot = line.split('\t', 1)
            guids.append(guid)
            input_snapshots.append(snapshot)
            i = snapshot.find('@')
            snapshot = snapshot[i+1:]
            permutation.append(snapshot[0:1])
            if snapshot[0:1] == 'd':
                included_guids.add(guid)
        return wbackup_zfs.Job().incremental_replication_steps(input_snapshots, guids, included_guids=included_guids,
                                                               force_convert_I_to_i=force_convert_I_to_i)


#############################################################################
class TestCLI(unittest.TestCase):

    def test_help(self):
        parser = wbackup_zfs.argument_parser()
        with self.assertRaises(SystemExit) as e:
            args = parser.parse_args(['--help'])
        self.assertEqual(0, e.exception.code)

    def test_version(self):
        parser = wbackup_zfs.argument_parser()
        with self.assertRaises(SystemExit) as e:
            args = parser.parse_args(['--version'])
        self.assertEqual(0, e.exception.code)


#############################################################################
class TestParseDatasetLocator(unittest.TestCase):
    def run_test(self, input, expected_user, expected_host, expected_dataset, expected_user_host,
                 expected_error):
        expected_status = 0 if not expected_error else 3
        passed = False

        # Run without validation
        user, host, user_host, pool, dataset = wbackup_zfs.parse_dataset_locator(input, validate=False)

        if user == expected_user and host == expected_host and dataset == expected_dataset and user_host == expected_user_host:
            passed = True

        # Rerun with validation
        status = 0
        try:
            wbackup_zfs.parse_dataset_locator(input, validate=True)
        except ValueError:
            status = 3

        if status != expected_status or (not passed):
            if status != expected_status:
                print("Validation Test failed:")
            else:
                print("Test failed:")
            print(f"input: {input}\nuser exp: '{expected_user}' vs '{user}'\nhost exp: '{expected_host}' vs '{host}'\nuserhost exp: '{expected_user_host}' vs '{user_host}'\ndataset exp: '{expected_dataset}' vs '{dataset}'")

    def runTest(self):
        # Input format is [[user@]host:]dataset
        # test columns indicate values for: input | user | host | dataset | userhost | validationError
        self.run_test("user@host.example.com:tank1/foo/bar", "user", "host.example.com", "tank1/foo/bar",
                      "user@host.example.com", False)
        self.run_test("joe@192.168.1.1:tank1/foo/bar:baz:boo", "joe", "192.168.1.1", "tank1/foo/bar:baz:boo",
                      "joe@192.168.1.1", False)
        self.run_test("tank1/foo/bar", "", "", "tank1/foo/bar", "", False)
        self.run_test("-:tank1/foo/bar:baz:boo", "", "", "tank1/foo/bar:baz:boo", "", False)
        self.run_test("host.example.com:tank1/foo/bar", "", "host.example.com", "tank1/foo/bar", "host.example.com",
                      False)
        self.run_test("root@host.example.com:tank1", "root", "host.example.com", "tank1", "root@host.example.com",
                      False)
        self.run_test("192.168.1.1:tank1/foo/bar", "", "192.168.1.1", "tank1/foo/bar", "192.168.1.1", False)
        self.run_test("user@192.168.1.1:tank1/foo/bar", "user", "192.168.1.1", "tank1/foo/bar", "user@192.168.1.1",
                      False)
        self.run_test("user@host_2024-01-02:a3:04:56:tank1/foo/bar", "user", "host_2024-01-02", "a3:04:56:tank1/foo/bar", "user@host_2024-01-02", False)
        self.run_test("user@host_2024-01-02:a3:04:56:tank1:/foo:/:bar", "user", "host_2024-01-02", "a3:04:56:tank1:/foo:/:bar", "user@host_2024-01-02", False)
        self.run_test("user@host_2024-01-02:03:04:56:tank1/foo/bar", "user", "host_2024-01-02", "03:04:56:tank1/foo/bar", "user@host_2024-01-02", True)
        self.run_test("user@localhost:tank1/foo/bar", "user", "localhost", "tank1/foo/bar", "user@localhost", False)
        self.run_test("host.local:tank1/foo/bar", "", "host.local", "tank1/foo/bar", "host.local", False)
        self.run_test("host.local:tank1/foo/bar", "", "host.local", "tank1/foo/bar", "host.local", False)
        self.run_test("user@host:", "user", "host", "", "user@host", True)
        self.run_test("@host:tank1/foo/bar", "", "host", "tank1/foo/bar", "host", False)
        self.run_test("@host:tank1/foo/bar", "", "host", "tank1/foo/bar", "host", False)
        self.run_test("@host:", "", "host", "", "host", True)
        self.run_test("user@:tank1/foo/bar", "", "user@", "tank1/foo/bar", "user@", True)
        self.run_test("user@:", "", "user@", "", "user@", True)
        self.run_test("@", "", "", "@", "", True)
        self.run_test("@foo", "", "", "@foo", "", True)
        self.run_test("@@", "", "", "@@", "", True)
        self.run_test(":::tank1:foo:bar:", "", "", ":::tank1:foo:bar:", "", True)
        self.run_test(":::tank1/bar", "", "", ":::tank1/bar", "", True)
        self.run_test(":::", "", "", ":::", "", True)
        self.run_test("::tank1/bar", "", "", "::tank1/bar", "", True)
        self.run_test("::", "", "", "::", "", True)
        self.run_test(":tank1/bar", "", "", ":tank1/bar", "", True)
        self.run_test(":", "", "", ":", "", True)
        self.run_test("", "", "", "", "", True)
        self.run_test("/", "", "", "/", "", True)
        self.run_test("tank//foo", "", "", "tank//foo", "", True)
        self.run_test("/tank1", "", "", "/tank1", "", True)
        self.run_test("tank1/", "", "", "tank1/", "", True)
        self.run_test(".", "", "", ".", "", True)
        self.run_test("..", "", "", "..", "", True)
        self.run_test("./tank", "", "", "./tank", "", True)
        self.run_test("../tank", "", "", "../tank", "", True)
        self.run_test("tank/..", "", "", "tank/..", "", True)
        self.run_test("tank/.", "", "", "tank/.", "", True)
        self.run_test("user@host.example.com:tank1/foo@bar", "user", "host.example.com", "tank1/foo@bar",
                      "user@host.example.com", True)
        self.run_test("user@host.example.com:tank1/foo#bar", "user", "host.example.com", "tank1/foo#bar",
                      "user@host.example.com", True)
        self.run_test("whitespace user@host.example.com:tank1/foo/bar", "whitespace user", "host.example.com",
                      "tank1/foo/bar", "whitespace user@host.example.com", True)
        self.run_test("user@whitespace\thost:tank1/foo/bar", "user", "whitespace\thost", "tank1/foo/bar",
                      "user@whitespace\thost", True)


#############################################################################
class TestReplaceCapturingGroups(unittest.TestCase):
    def replace_capturing_group(self, regex):
        return wbackup_zfs.replace_capturing_groups_with_non_capturing_groups(regex)

    def test_basic_case(self):
        self.assertEqual(self.replace_capturing_group('(abc)'), '(?:abc)')

    def test_nested_groups(self):
        self.assertEqual(self.replace_capturing_group('(a(bc)d)'), '(?:a(?:bc)d)')

    def test_preceding_backslash(self):
        self.assertEqual(self.replace_capturing_group('\\(abc)'), '\\(abc)')

    def test_group_starting_with_question_mark(self):
        self.assertEqual(self.replace_capturing_group('(?abc)'), '(?abc)')

    def test_multiple_groups(self):
        self.assertEqual(self.replace_capturing_group('(abc)(def)'), '(?:abc)(?:def)')

    def test_mixed_cases(self):
        self.assertEqual(self.replace_capturing_group('a(bc\\(de)f(gh)?i'),
                         'a(?:bc\\(de)f(?:gh)?i')

    def test_empty_group(self):
        self.assertEqual(self.replace_capturing_group('()'), '(?:)')

    def test_group_with_named_group(self):
        self.assertEqual(self.replace_capturing_group('(?P<name>abc)'), '(?P<name>abc)')

    def test_group_with_non_capturing_group(self):
        self.assertEqual(self.replace_capturing_group('(a(?:bc)d)'), '(?:a(?:bc)d)')

    def test_group_with_lookahead(self):
        self.assertEqual(self.replace_capturing_group('(abc)(?=def)'), '(?:abc)(?=def)')

    def test_group_with_lookbehind(self):
        self.assertEqual(self.replace_capturing_group('(?<=abc)(def)'), '(?<=abc)(?:def)')

    def test_escaped_characters(self):
        pattern = re.escape('(abc)')
        self.assertEqual(self.replace_capturing_group(pattern), pattern)

    def test_complex_pattern_with_escape(self):
        complex_pattern = re.escape('(a[b]c{d}e|f.g)')
        self.assertEqual(self.replace_capturing_group(complex_pattern), complex_pattern)

    def test_complex_pattern(self):
        complex_pattern = '(a[b]c{d}e|f.g)(h(i|j)k)?(\\(l\\))'
        expected_result = '(?:a[b]c{d}e|f.g)(?:h(?:i|j)k)?(?:\\(l\\))'
        self.assertEqual(self.replace_capturing_group(complex_pattern), expected_result)


#############################################################################
class TestFileOrLiteralAction(unittest.TestCase):

    def setUp(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('--input', nargs='+', action=wbackup_zfs.FileOrLiteralAction)

    def test_file_input(self):
        with patch('builtins.open', mock_open(read_data='file content line 1\nfile content line 2\n')):
            args = self.parser.parse_args(['--input', '@testfile'])
            self.assertEqual(args.input, ['file content line 1', 'file content line 2'])

    def test_direct_value(self):
        args = self.parser.parse_args(['--input', 'literalvalue'])
        self.assertEqual(args.input, ['literalvalue'])

    def test_mixed_input(self):
        with patch('builtins.open', mock_open(read_data='file content line 1\nfile content line 2\n')):
            args = self.parser.parse_args(['--input', 'literalvalue', '@testfile'])
            self.assertEqual(args.input, ['literalvalue', 'file content line 1', 'file content line 2'])

    def test_file_not_found(self):
        with patch('builtins.open', side_effect=FileNotFoundError):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(['--input', '@nonexistentfile'])

    def test_option_not_specified(self):
        args = self.parser.parse_args([])
        self.assertIsNone(args.input)


#############################################################################
class TestCheckRange(unittest.TestCase):

    def test_valid_range_min_max(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=int, action=CheckRange, min=0, max=100)
        args = parser.parse_args(['--age', '50'])
        self.assertEqual(args.age, 50)

    def test_valid_range_inf_sup(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=int, action=CheckRange, inf=0, sup=100)
        args = parser.parse_args(['--age', '50'])
        self.assertEqual(args.age, 50)

    def test_invalid_range_min_max(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=int, action=CheckRange, min=0, max=100)
        with self.assertRaises(SystemExit):
            parser.parse_args(['--age', '-1'])

    def test_invalid_range_inf_sup(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=int, action=CheckRange, inf=0, sup=100)
        with self.assertRaises(SystemExit):
            parser.parse_args(['--age', '101'])

    def test_invalid_combination_min_inf(self):
        with self.assertRaises(ValueError):
            parser = argparse.ArgumentParser()
            parser.add_argument('--age', type=int, action=CheckRange, min=0, inf=100)

    def test_invalid_combination_max_sup(self):
        with self.assertRaises(ValueError):
            parser = argparse.ArgumentParser()
            parser.add_argument('--age', type=int, action=CheckRange, max=0, sup=100)

    def test_valid_float_range_min_max(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=float, action=CheckRange, min=0.0, max=100.0)
        args = parser.parse_args(['--age', '50.5'])
        self.assertEqual(args.age, 50.5)

    def test_invalid_float_range_min_max(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=float, action=CheckRange, min=0.0, max=100.0)
        with self.assertRaises(SystemExit):
            parser.parse_args(['--age', '-0.1'])

    def test_valid_edge_case_min(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=float, action=CheckRange, min=0.0, max=100.0)
        args = parser.parse_args(['--age', '0.0'])
        self.assertEqual(args.age, 0.0)

    def test_valid_edge_case_max(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=float, action=CheckRange, min=0.0, max=100.0)
        args = parser.parse_args(['--age', '100.0'])
        self.assertEqual(args.age, 100.0)

    def test_invalid_edge_case_sup(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=float, action=CheckRange, inf=0.0, sup=100.0)
        with self.assertRaises(SystemExit):
            parser.parse_args(['--age', '100.0'])

    def test_invalid_edge_case_inf(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=float, action=CheckRange, inf=0.0, sup=100.0)
        with self.assertRaises(SystemExit):
            parser.parse_args(['--age', '0.0'])

    def test_no_range_constraints(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=int, action=CheckRange)
        args = parser.parse_args(['--age', '150'])
        self.assertEqual(args.age, 150)

    def test_no_range_constraints_float(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=float, action=CheckRange)
        args = parser.parse_args(['--age', '150.5'])
        self.assertEqual(args.age, 150.5)

    def test_very_large_value(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=int, action=CheckRange, max=10**18)
        args = parser.parse_args(['--age', '999999999999999999'])
        self.assertEqual(args.age, 999999999999999999)

    def test_very_small_value(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=int, action=CheckRange, min=-10**18)
        args = parser.parse_args(['--age', '-999999999999999999'])
        self.assertEqual(args.age, -999999999999999999)

    def test_default_interval(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=int, action=CheckRange)
        action = CheckRange(option_strings=['--age'], dest='age')
        self.assertEqual(action.interval(), 'valid range: (-infinity, +infinity)')

    def test_interval_with_inf_sup(self):
        action = CheckRange(option_strings=['--age'], dest='age', inf=0, sup=100)
        self.assertEqual(action.interval(), 'valid range: (0, 100)')

    def test_interval_with_min_max(self):
        action = CheckRange(option_strings=['--age'], dest='age', min=0, max=100)
        self.assertEqual(action.interval(), 'valid range: [0, 100]')

    def test_interval_with_min(self):
        action = CheckRange(option_strings=['--age'], dest='age', min=0)
        self.assertEqual(action.interval(), 'valid range: [0, +infinity)')

    def test_interval_with_max(self):
        action = CheckRange(option_strings=['--age'], dest='age', max=100)
        self.assertEqual(action.interval(), 'valid range: (-infinity, 100]')

    def test_call_without_range_constraints(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--age', type=int, action=CheckRange)
        args = parser.parse_args(['--age', '50'])
        self.assertEqual(args.age, 50)


#############################################################################
def create_datasets(path, props=None):
    create_dataset(src_root_dataset, path, props=props)
    return create_dataset(dst_root_dataset, path, props=props)


def recreate_dataset(dataset, props=None):
    if dataset_exists(dataset):
        destroy(dataset, recursive=True)
    return create_dataset(dataset, props=props)


def detect_zpool_features(location, pool):
    cmd = "zpool get -Hp -o property,value all".split(' ') + [pool]
    lines = run_cmd(cmd)
    props = {line.split('\t', 1)[0]: line.split('\t', 1)[1] for line in lines}
    features = {k: v for k, v in props.items() if k.startswith('feature@')}
    zpool_features[location] = features


def is_zpool_feature_enabled_or_active(location, feature):
    value = zpool_features[location].get(feature, None)
    return value == 'active' or value == 'enabled'


def is_zpool_bookmarks_feature_enabled_or_active(location):
    return (is_zpool_feature_enabled_or_active(location, 'feature@bookmark_v2')
            and is_zpool_feature_enabled_or_active(location, 'feature@bookmark_written'))


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


def is_solaris_zfs():
    return platform.system() == 'SunOS'


if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(TestParseDatasetLocator())
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestReplaceCapturingGroups))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestFileOrLiteralAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCheckRange))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCLI))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(ExcludeSnapshotRegexValidationCase))
    suite.addTest(ParametrizedTestCase.parametrize(ExcludeSnapshotRegexTestCase, {'verbose': True}))


    # for ssh_mode in []:
    for ssh_mode in ['local']:
    # for ssh_mode in ['pull-push']:
    # for ssh_mode in ['local', 'pull-push']:
        # for min_transfer_size in [0, 1024 ** 2]:
        for min_transfer_size in [1024 ** 2]:
            for affix in [""]:
            # for affix in [".  -"]:
            # for affix in ["", ".  -"]:
            #     no_privilege_elevation_modes = []
                no_privilege_elevation_modes = [False]
                if os.geteuid() != 0:
                    no_privilege_elevation_modes.append(True)
                for no_privilege_elevation in no_privilege_elevation_modes:
                    # for encrypted_dataset in [False]:
                    for encrypted_dataset in [False, True]:
                        params = {
                            'ssh_mode': ssh_mode,
                            'verbose': True,
                            'min_transfer_size': min_transfer_size,
                            'affix': affix,
                            'skip_missing_snapshots': 'false',
                            'no_privilege_elevation': no_privilege_elevation,
                            'encrypted_dataset': encrypted_dataset,
                        }
                        # params = {'ssh_mode': 'pull-push', 'verbose': True, 'min_transfer_size': min_transfer_size}
                        # params = {'verbose': True}
                        # params = None
                        # suite.addTest(ParametrizedTestCase.parametrize(IsolatedTestCase, params))
                        suite.addTest(ParametrizedTestCase.parametrize(LocalTestCase, params))

    # for ssh_mode in []:
    # for ssh_mode in ['pull-push']:
    # for ssh_mode in ['local']:
    for ssh_mode in ['local', 'pull-push']:
    # for ssh_mode in ['local', 'pull-push', 'push', 'pull']:
    #     for min_transfer_size in [1024 ** 2]:
        for min_transfer_size in [0, 1024 ** 2]:
        #     for affix in [""]:
        #     for affix in [".  -"]:
            for affix in ["", ".  -"]:
                no_privilege_elevation_modes = [False]
                # no_privilege_elevation_modes = []
                # if os.geteuid() != 0:
                #     no_privilege_elevation_modes.append(True)
                for no_privilege_elevation in no_privilege_elevation_modes:
                    for encrypted_dataset in [False]:
                        params = {
                            'ssh_mode': ssh_mode,
                            'verbose': True,
                            'min_transfer_size': min_transfer_size,
                            'affix': affix,
                            'skip_missing_snapshots': 'false',
                            'no_privilege_elevation': no_privilege_elevation,
                            'encrypted_dataset': encrypted_dataset,
                        }
                        suite.addTest(ParametrizedTestCase.parametrize(RemoteTestCase, params))

    failfast = False if os.getenv('CI') else True  # no need to fail fast when run within GitHub Action
    print(f"Running in failfast mode: {failfast} ...")
    result = unittest.TextTestRunner(failfast=failfast, verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
