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
"""Aggregates the various integration test classes into a single `unittest.TestSuite`."""

from __future__ import (
    annotations,
)
import os
import unittest

from bzfs_tests.abstract_testcase import (
    AbstractTestCase,
)
from bzfs_tests.itest.ibase import (
    ParametrizedTestCase,
)
from bzfs_tests.itest.test_incremental_send_steps import (
    IncrementalSendStepsTestCase,
)
from bzfs_tests.itest.test_lima_ubuntu_24_04_sh import (
    TestLimaUbuntuScript,
)
from bzfs_tests.itest.test_local import (
    LocalTestCase,
)
from bzfs_tests.itest.test_remote import (
    FullRemoteTestCase,
    MinimalRemoteTestCase,
)
from bzfs_tests.itest.test_smoke_adhoc import (
    AdhocTestCase,
    SmokeTestCase,
)
from bzfs_tests.itest.test_ssh import (
    TestSSHLatency,
    TestSSHMasterIntermittentFailure,
)


def suite() -> unittest.TestSuite:
    ttype = AbstractTestCase()
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestLimaUbuntuScript))
    if not (ttype.is_smoke_test or ttype.is_functional_test or ttype.is_adhoc_test):
        suite.addTest(ParametrizedTestCase.parametrize(IncrementalSendStepsTestCase, {"verbose": True}))
        suite.addTest(ParametrizedTestCase.parametrize(TestSSHLatency))
        suite.addTest(ParametrizedTestCase.parametrize(TestSSHMasterIntermittentFailure))

    # for ssh_mode in ["pull-push"]:
    # for ssh_mode in ["local", "pull-push"]:
    # for ssh_mode in []:
    for ssh_mode in ["local"]:
        for min_pipe_transfer_size in [1024**2]:
            for affix in [""]:
                # no_privilege_elevation_modes = []
                no_privilege_elevation_modes = [False]
                if not (os.getuid() == 0 or ttype.is_smoke_test or ttype.is_functional_test or ttype.is_adhoc_test):
                    no_privilege_elevation_modes.append(True)
                for no_privilege_elevation in no_privilege_elevation_modes:
                    encrypted_datasets = [False]
                    if not (ttype.is_smoke_test or ttype.is_functional_test or ttype.is_adhoc_test):
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
                        if ttype.is_smoke_test:
                            suite.addTest(ParametrizedTestCase.parametrize(SmokeTestCase, params))
                        elif ttype.is_adhoc_test:
                            suite.addTest(ParametrizedTestCase.parametrize(AdhocTestCase, params))
                        else:
                            suite.addTest(ParametrizedTestCase.parametrize(LocalTestCase, params))
    if ttype.is_smoke_test or ttype.is_adhoc_test:
        return suite

    # ssh_modes = []
    # ssh_modes = ["pull-push"]
    # ssh_modes = ["local", "pull-push", "push", "pull"]
    # ssh_modes = ["local"]
    ssh_modes = ["local", "pull-push"]
    ssh_modes = ["local"] if ttype.is_functional_test else ssh_modes
    for ssh_mode in ssh_modes:
        min_pipe_transfer_sizes = [0, 1024**2]
        min_pipe_transfer_sizes = [0] if ttype.is_functional_test else min_pipe_transfer_sizes
        for min_pipe_transfer_size in min_pipe_transfer_sizes:
            # for affix in [""]:
            # for affix in ["", ".  -"]:
            for affix in [".  -"]:
                no_privilege_elevation_modes = [False]
                for no_privilege_elevation in no_privilege_elevation_modes:
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
                        suite.addTest(ParametrizedTestCase.parametrize(FullRemoteTestCase, params))

    if os.getuid() != 0 and not ttype.is_functional_test:
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
