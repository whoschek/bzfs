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
"""Unit tests for computing incremental send steps via incremental_send_steps(); Verifies that snapshot replication uses
minimal sends while respecting dependency order."""

from __future__ import (
    annotations,
)
import itertools
import unittest
from collections import (
    defaultdict,
)

import bzfs_main.incremental_send_steps
from bzfs_main.incremental_send_steps import (
    send_step_to_str,
)
from bzfs_tests.tools import (
    stop_on_failure_subtest,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestIncrementalSendSteps,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestIncrementalSendSteps(unittest.TestCase):

    def test_basic1(self) -> None:
        input_snapshots = ["d1", "h1", "d2", "d3", "d4"]
        expected_results = ["d1", "d2", "d3", "d4"]
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_basic2(self) -> None:
        input_snapshots = ["d1", "d2", "h1", "d3", "d4"]
        expected_results = ["d1", "d2", "d3", "d4"]
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_basic3(self) -> None:
        """If latest common snapshot is an hourly, dst contains that hourly."""
        input_snapshots: list[str] = ["h0", "h1", "d1", "d2", "h2", "d3", "d4"]
        expected_results: list[str] = ["h0", "d1", "d2", "d3", "d4"]
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_basic4(self) -> None:
        input_snapshots: list[str] = ["d1"]
        expected_results: list[str] = ["d1"]
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_basic5(self) -> None:
        input_snapshots: list[str] = []
        expected_results: list[str] = []
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_basic6(self) -> None:
        input_snapshots: list[str] = ["h1"]
        expected_results: list[str] = ["h1"]
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_validate_snapshot_series_excluding_hourlies_with_permutations(self) -> None:
        for i, testcase in enumerate(self.permute_snapshot_series()):
            with stop_on_failure_subtest(i=i):
                self.validate_incremental_send_steps(testcase[None], testcase["d"])

    def test_send_step_to_str(self) -> None:
        send_step_to_str(("-I", "d@s1", "d@s3"))

    def test_bookmark_base_skips_excluded_next_snapshot(self) -> None:
        """Verify correct behavior when the latest common snapshot is actually a bookmark and the immediate next snapshot is
        excluded by include/exclude policy.

        Purpose: Ensure planning starts from the bookmark (ds#h0) and targets the first included snapshot (ds@d1)
        even if the next snapshot (ds@h1) is excluded via included_guids. The test asserts the first step uses -i
        from the bookmark to d1, and that to_snapshots include only daily snapshots ["d1", "d2"], never the
        excluded hourly h1.

        Assumptions: The latest common snapshot is a bookmark rather than a snapshot. included_guids is the set of
        snapshots eligible for replication; bookmarks are denoted with # and ZFS supports zfs send -i <bookmark> <snapshot>.

        Design rationale: Starting from a bookmark requires -i (not -I) because the base is not a snapshot.
        Skipping the excluded next snapshot must not stall planning or pick an unsafe base; it should advance to the
        first allowed snapshot while maintaining dependency order. We validate invariants across resume and
        non-resume modes and with optional force_convert_I_to_i to ensure semantics remain stable.

        This guards against regressions where the planner includes excluded snapshots, pairs a bookmark with -I, or
        targets the wrong snapshot when the first candidate after the base is filtered out. Correctly.
        """
        src_snapshots = ["ds#h0", "ds@h1", "ds@d1", "ds@d2"]
        src_guids = ["1", "2", "3", "4"]
        included_guids = {"3", "4"}  # include only d1, d2
        for is_resume in [False, True]:
            for force_convert_I_to_i in [False, True]:  # noqa: N806
                steps = bzfs_main.incremental_send_steps.incremental_send_steps(
                    src_snapshots,
                    src_guids,
                    included_guids=included_guids,
                    is_resume=is_resume,
                    force_convert_I_to_i=force_convert_I_to_i,
                )
                # First step must originate from the bookmark and go to the first included snapshot
                self.assertGreaterEqual(len(steps), 1)
                first = steps[0]
                self.assertEqual("-i", first[0])  # bookmark start requires -i
                self.assertEqual("ds#h0", first[1])
                self.assertEqual("ds@d1", first[2])
                self.assertEqual(["ds@d1"], first[3])

                # Validate that only d1 and d2 are replicated; h1 is never in any to_snapshots
                to_names = [snap[snap.find("@") + 1 :] for _, _, _, to in steps for snap in to]
                self.assertEqual(["d1", "d2"], to_names)
                self.assertNotIn("h1", to_names)

    def permute_snapshot_series(self, max_length: int = 9) -> list[defaultdict[str | None, list[str]]]:
        """
        Simulates a series of hourly and daily snapshots. At the end, makes a backup while excluding hourly
        snapshots from replication. The expectation is that after replication dst contains all daily snapshots
        and no hourly snapshots.
        Example snapshot series: d1, h1, d2, d3, d4 --> expected dst output: d1, d2, d3, d4
        where
        d1 = first daily snapshot,  dN = n-th daily snapshot
        h1 = first hourly snapshot, hN = n-th hourly snapshot

        We test all possible permutations of series of length L=[0..max_length] snapshots
        """
        assert max_length >= 0
        testcases: list[defaultdict[str | None, list[str]]] = []
        for L in range(max_length + 1):  # noqa: N806
            for N in range(L + 1):  # noqa: N806
                steps: str = "d" * N + "h" * (L - N)  # e.g. "ddddh"
                # compute a permutation of several 'd' and 'h' chars that represents the snapshot series
                for permutation in sorted(set(itertools.permutations(steps, len(steps)))):  # e.g. permutation = "dhddd"
                    snaps: defaultdict[str | None, list[str]] = defaultdict(list)
                    count: defaultdict[str, int] = defaultdict(int)
                    for char in permutation:
                        count[char] += 1  # tag snapshots with a monotonically increasing number within each category
                        char_count: str = f"{count[char]:01}" if max_length < 10 else f"{count[char]:02}"  # zero pad number
                        snapshot: str = f"{char}{char_count}"
                        snaps[None].append(snapshot)
                        snaps[char].append(snapshot)  # represents expected results for test verification, e.g. [d1,d2,d3,d4]
                    h = permutation.index("h") if "h" in permutation else -1
                    d = permutation.index("d") if "d" in permutation else -1
                    if h >= 0 and (d < 0 or h < d):
                        snaps["d"].insert(0, snaps["h"][0])  # if latest common snap is an hourly, dst contains that hourly
                    testcases.append(snaps)
        return testcases

    def validate_incremental_send_steps(self, input_snapshots: list[str], expected_results: list[str]) -> None:
        """Computes steps to incrementally replicate the daily snapshots of the given daily and/or hourly input snapshots.

        Applies the steps and compares the resulting destination snapshots with the expected results.
        """
        for is_resume in [False, True]:  # via --no-resume-recv
            for src_dataset in ["", "s@"]:
                for force_convert_I_to_i in [False, True]:  # noqa: N806
                    steps: list[tuple] = self.incremental_send_steps1(
                        input_snapshots,
                        src_dataset=src_dataset,
                        is_resume=is_resume,
                        force_convert_I_to_i=force_convert_I_to_i,
                    )
                    # print("====================================================")
                    # print("input_snapshots: " + ",".join(input_snapshots))
                    # print("steps: " + ",".join([self.send_step_to_str(step) for step in steps]))
                    # print("expected_results:" + ",".join(expected_results))
                    output_snapshots = self.apply_incremental_send_steps(steps, input_snapshots)
                    # print("output_snapshots:" + ",".join(output_snapshots))
                    self.assertListEqual(expected_results, output_snapshots)
                    all_to_snapshots: list[str] = []
                    for incr_flag, start_snapshot, end_snapshot, to_snapshots in steps:  # noqa: B007
                        self.assertIn(incr_flag, ["-I", "-i"])
                        self.assertGreaterEqual(len(to_snapshots), 1)
                        all_to_snapshots += [snapshot[snapshot.find("@") + 1 :] for snapshot in to_snapshots]
                    self.assertListEqual(expected_results[1:], all_to_snapshots)

    def send_step_to_str(self, step: tuple) -> str:
        # return str(step)
        return str(step[1]) + ("-" if step[0] == "-I" else ":") + str(step[2])

    def apply_incremental_send_steps(self, steps: list[tuple], input_snapshots: list[str]) -> list[str]:
        """Simulates replicating (a subset of) the given input_snapshots to a destination, according to the given steps.

        Returns the subset of snapshots that have actually been replicated to the destination.
        """
        output_snapshots: list[str] = []
        for i, (incr_flag, start_snapshot, end_snapshot, to_snapshots_) in enumerate(steps):  # noqa: B007
            start_snapshot = start_snapshot[start_snapshot.find("@") + 1 :]
            end_snapshot = end_snapshot[end_snapshot.find("@") + 1 :]
            start: int = input_snapshots.index(start_snapshot)
            end: int = input_snapshots.index(end_snapshot)
            if i == 0:
                output_snapshots.append(input_snapshots[start])
            if incr_flag == "-I":
                for j in range(start + 1, end + 1):
                    output_snapshots.append(input_snapshots[j])
            else:
                output_snapshots.append(input_snapshots[end])
        if len(steps) == 0 and len(input_snapshots) > 0:
            output_snapshots.append(input_snapshots[0])  # dst contains at least the latest common snapshot
        return output_snapshots

    def incremental_send_steps1(
        self,
        input_snapshots: list[str],
        src_dataset: str,
        is_resume: bool = False,
        force_convert_I_to_i: bool = False,  # noqa: N803
    ) -> list[tuple]:
        origin_src_snapshots_with_guids: list[str] = []
        guid = 1
        for snapshot in input_snapshots:
            origin_src_snapshots_with_guids.append(f"{guid}\t{src_dataset}{snapshot}")
            guid += 1
        return self.incremental_send_steps2(
            origin_src_snapshots_with_guids, is_resume=is_resume, force_convert_I_to_i=force_convert_I_to_i
        )

    def incremental_send_steps2(
        self,
        origin_src_snapshots_with_guids: list[str],
        is_resume: bool = False,
        force_convert_I_to_i: bool = False,  # noqa: N803
    ) -> list[tuple]:
        guids: list[str] = []
        input_snapshots: list[str] = []
        included_guids: set[str] = set()
        for line in origin_src_snapshots_with_guids:
            guid, snapshot = line.split("\t", 1)
            guids.append(guid)
            input_snapshots.append(snapshot)
            i = snapshot.find("@")
            snapshot = snapshot[i + 1 :]
            if snapshot[0:1] == "d":
                included_guids.add(guid)
        return bzfs_main.incremental_send_steps.incremental_send_steps(
            input_snapshots,
            guids,
            included_guids=included_guids,
            is_resume=is_resume,
            force_convert_I_to_i=force_convert_I_to_i,
        )
