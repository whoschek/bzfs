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
import time
import unittest
from unittest.mock import (
    MagicMock,
    patch,
)

import bzfs_main.detect
from bzfs_main import bzfs
from bzfs_main.connection import (
    DEDICATED,
    SHARED,
    ConnectionPools,
)
from bzfs_main.detect import (
    RemoteConfCacheItem,
    detect_available_programs,
)
from bzfs_tests.abstract_test import AbstractTest


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestRemoteConfCache,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestRemoteConfCache(AbstractTest):

    def test_remote_conf_cache_hit_skips_detection(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        p = bzfs.Params(args)
        p.log = MagicMock()
        job = bzfs.Job()
        job.params = p
        p.src = bzfs.Remote("src", args, p)
        p.dst = bzfs.Remote("dst", args, p)
        p.src.ssh_host = "host"
        p.src.ssh_user_host = "host"
        p.dst.ssh_host = "host2"
        p.dst.ssh_user_host = "host2"
        job.params.available_programs["local"] = {"ssh": ""}
        pools = ConnectionPools(p.src, {SHARED: 1, DEDICATED: 1})
        item = RemoteConfCacheItem(pools, {"os": "Linux", "ssh": ""}, {"feat": "on"}, time.monotonic_ns())
        job.remote_conf_cache[p.src.cache_key()] = item
        job.remote_conf_cache[p.dst.cache_key()] = item
        with patch.object(bzfs_main.detect, "detect_available_programs_remote") as d1, patch.object(
            bzfs_main.detect, "detect_zpool_features"
        ) as d2:
            detect_available_programs(job)
            d1.assert_not_called()
            d2.assert_not_called()

    def test_remote_conf_cache_miss_runs_detection(self) -> None:
        args = self.argparser_parse_args(["src", "dst", "--daemon-remote-conf-cache-ttl", "10 milliseconds"])
        p = bzfs.Params(args)
        p.log = MagicMock()
        job = bzfs.Job()
        job.params = p
        p.src = bzfs.Remote("src", args, p)
        p.dst = bzfs.Remote("dst", args, p)
        p.src.ssh_host = "host"
        p.src.ssh_user_host = "host"
        p.dst.ssh_host = "host2"
        p.dst.ssh_user_host = "host2"
        job.params.available_programs["local"] = {"ssh": ""}
        pools = ConnectionPools(p.src, {SHARED: 1, DEDICATED: 1})
        expired_ts = time.monotonic_ns() - p.remote_conf_cache_ttl_nanos - 1
        item = RemoteConfCacheItem(pools, {"os": "Linux"}, {"feat": "on"}, expired_ts)
        job.remote_conf_cache[p.src.cache_key()] = item
        job.remote_conf_cache[p.dst.cache_key()] = item
        with patch.object(bzfs_main.detect, "detect_available_programs_remote") as d1, patch.object(
            bzfs_main.detect, "detect_zpool_features"
        ) as d2:
            d1.side_effect = lambda p, r, programs, host: programs.__setitem__(r.location, {"ssh": ""})
            d2.side_effect = lambda p, r: job.params.zpool_features.__setitem__(r.location, {"feat": "on"})
            detect_available_programs(job)
            self.assertEqual(d1.call_count, 2)
            self.assertEqual(d2.call_count, 2)
