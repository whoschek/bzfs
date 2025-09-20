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
"""Computes efficient incremental ZFS send/receive steps.

This helper derives a minimal sequence of 'zfs send' commands from a list of snapshots and GUIDs to include. It favors fewer
send steps for performance and works around resume and bookmark limitations.
"""

from __future__ import (
    annotations,
)


def incremental_send_steps(
    src_snapshots: list[str],  # [0] = the latest common snapshot (which may be a bookmark), followed by all src snapshots
    # (that are not a bookmark) that are more recent than that.
    src_guids: list[str],  # the guid of each item in src_snapshots
    included_guids: set[str],  # the guid of each snapshot (not bookmark!) that is included by --include/exclude-snapshot-*
    is_resume: bool,
    force_convert_I_to_i: bool,  # noqa: N803
) -> list[tuple[str, str, str, list[str]]]:  # the 4th tuple item lists the to_snapshots for that step, aiding trace/logging
    """Computes steps to incrementally replicate the given src snapshots with the given src_guids such that we include
    intermediate src snapshots that pass the policy specified by --{include,exclude}-snapshot-* (represented here by
    included_guids), using an optimal series of -i/-I send/receive steps that skip excluded src snapshots. The steps are
    optimal in the sense that no solution with fewer steps exists. A step corresponds to a single ZFS send/receive operation.
    Fewer steps translate to better performance, especially when sending many small snapshots. For example, 1 step that sends
    100 small snapshots in a single operation is much faster than 100 steps that each send only 1 such snapshot per ZFS
    send/receive operation.

    Examples that skip hourly snapshots and only include daily snapshots for replication:

    Example A where d1 is the latest common snapshot:
    src = [d1, h1, d2, d3, d4] (d is daily, h is hourly) --> dst = [d1, d2, d3, d4] via
    -i d1:d2 (i.e. exclude h1; '-i' and ':' indicate 'skip intermediate snapshots')
    -I d2-d4 (i.e. also include d3; '-I' and '-' indicate 'include intermediate snapshots')

    Example B where h0 is the latest common snapshot:
    src = [h0, m0, d1, h1, d2, d3, d4] (d is daily, h is hourly) --> dst = [h0, d1, d2, d3, d4] via
    -i h0:d1 (i.e. exclude m0; '-i' and ':' indicate 'skip intermediate snapshots')
    -i d1:d2 (i.e. exclude h1; '-i' and ':' indicate 'skip intermediate snapshots')
    -I d2-d4 (i.e. also include d3; '-I' and '-' indicate 'include intermediate snapshots')

    Example C where h0 is the latest common snapshot:
    src = [h0, m0] (d is daily, h is hourly) --> dst = [h0] via returning an empty list

    * The force_convert_I_to_i param is necessary as a work-around for https://github.com/openzfs/zfs/issues/16394
    * The 'zfs send' CLI with a bookmark as starting snapshot does not (yet) support including intermediate
    src_snapshots via -I flag per https://github.com/openzfs/zfs/issues/12415. Thus, if the replication source
    is a bookmark we convert a -I step to a -i step followed by zero or more -i/-I steps.
    * The is_resume param is necessary as 'zfs send -t' does not support sending more than a single snapshot
    on resuming a previously interrupted 'zfs receive -s'. Thus, here too, we convert a -I step to a -i step
    followed by zero or more -i/-I steps.
    """

    def append_run(i: int, label: str) -> int:
        """Appends a run of snapshots as one or more send steps."""
        # step = ("-I", src_snapshots[start], src_snapshots[i], i - start)
        # print(f"{label} {self.send_step_to_str(step)}")
        is_not_resume: bool = len(steps) > 0 or not is_resume
        if i - start > 1 and (not force_convert_I_to_i) and "@" in src_snapshots[start] and is_not_resume:
            steps.append(("-I", src_snapshots[start], src_snapshots[i], src_snapshots[start + 1 : i + 1]))
        elif "@" in src_snapshots[start] and is_not_resume:
            for j in range(start, i):  # convert -I step to -i steps
                steps.append(("-i", src_snapshots[j], src_snapshots[j + 1], src_snapshots[j + 1 : j + 2]))
        else:  # it's a bookmark src or zfs send -t; convert -I step to -i step followed by zero or more -i/-I steps
            steps.append(("-i", src_snapshots[start], src_snapshots[start + 1], src_snapshots[start + 1 : start + 2]))
            i = start + 1
        return i - 1

    assert len(src_guids) == len(src_snapshots)
    assert isinstance(included_guids, set)
    steps = []
    guids: list[str] = src_guids
    n = len(guids)
    i = 0
    start = i
    while i < n and guids[i] not in included_guids:  # skip hourlies
        i += 1
    if i < n and i != start:
        # the latest common snapshot is an hourly or a bookmark, followed by zero or more hourlies, followed by a daily
        step = ("-i", src_snapshots[start], src_snapshots[i], src_snapshots[i : i + 1])
        steps.append(step)

    while i < n:
        assert guids[i] in included_guids  # it's a daily
        start = i
        i += 1
        while i < n and guids[i] in included_guids:  # skip dailies
            i += 1
        if i < n:
            if i - start == 1:
                # it's a single daily (that was already replicated) followed by an hourly
                i += 1
                while i < n and guids[i] not in included_guids:  # skip hourlies
                    i += 1
                if i < n:
                    assert start != i
                    step = ("-i", src_snapshots[start], src_snapshots[i], src_snapshots[i : i + 1])
                    # print(f"r1 {self.send_step_to_str(step)}")
                    steps.append(step)
                    i -= 1
            else:  # it's a run of more than one daily
                i -= 1
                assert start != i
                i = append_run(i, "r2")
        else:  # finish up run of trailing dailies
            i -= 1
            if start != i:
                i = append_run(i, "r3")
        i += 1
    return steps


def send_step_to_str(step: tuple[str, str, str]) -> str:
    """Returns a readable representation of an incremental send step."""
    # return str(step[1]) + ('-' if step[0] == '-I' else ':') + str(step[2])
    return str(step)
