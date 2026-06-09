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
"""Integration tests and utilities for replication.py#_sanitize_recv_opts_for_dataset_type()."""

from __future__ import (
    annotations,
)
import shlex
import subprocess
import time
from collections.abc import (
    Mapping,
)
from dataclasses import (
    dataclass,
)
from typing import (
    Final,
    final,
)

from bzfs_main.replication import (
    _ZFS_RECV_PROPS_REJECTED_ON_FILESYSTEM,
    _ZFS_RECV_PROPS_REJECTED_ON_ZVOL,
)
from bzfs_tests.itest import (
    ibase,
)
from bzfs_tests.itest.ibase import (
    SUDO_CMD,
    IntegrationTestCase,
    is_zfs_at_least_2_1_0,
)
from bzfs_tests.zfs_util import (
    create_filesystem,
    create_volume,
    dataset_exists,
    destroy,
    take_snapshot,
    zfs_get,
    zfs_set,
)

_ALL_RECV_PROPS: Final[tuple[str, ...]] = tuple(
    sorted(
        set().union(
            *(_ZFS_RECV_PROPS_REJECTED_ON_FILESYSTEM.values()),
            *(_ZFS_RECV_PROPS_REJECTED_ON_ZVOL.values()),
        )
    )
)
_DATASET_TYPES: Final[tuple[str, ...]] = ("filesystem", "volume")
_STREAM_KINDS: Final[tuple[str, ...]] = ("full", "incremental")
_RECV_FLAGS: Final[tuple[str, ...]] = ("-o", "-x")
_RECV_FLAG_PATH_TOKENS: Final[dict[str, str]] = {"-o": "o", "-x": "x"}
_BUCKET_NAMES: Final[dict[str, dict[str, str]]] = {
    "-o": {
        "filesystem_only": "_ZFS_RECV_PROPS_REJECTED_ON_ZVOL['-o']",
        "volume_only": "_ZFS_RECV_PROPS_REJECTED_ON_FILESYSTEM['-o']",
    },
    "-x": {
        "filesystem_only": "_ZFS_RECV_PROPS_REJECTED_ON_ZVOL['-x']",
        "volume_only": "_ZFS_RECV_PROPS_REJECTED_ON_FILESYSTEM['-x']",
    },
}
_RECV_O_VALUE_OVERRIDES: Final[dict[str, str]] = {
    "mountpoint": "none",
}
_RECV_PROP_BUCKETS: Final[Mapping[str, Mapping[str, frozenset[str]]]] = {
    "filesystem": _ZFS_RECV_PROPS_REJECTED_ON_FILESYSTEM,
    "volume": _ZFS_RECV_PROPS_REJECTED_ON_ZVOL,
}
_EXPECTED_REJECT_EXIT_CODE: Final[int] = 1
_EXPECTED_ACCEPT_EXIT_CODE: Final[int] = 0


@dataclass(frozen=True)
class _ProbeResult:
    propname: str
    recv_flag: str
    dataset_type: str
    stream_kind: str
    exit_code: int


@final
class SanitizeRecvOptsForDatasetTypeTestCase(IntegrationTestCase):
    """Empirically probes plain `zfs receive` for properties filtered by _sanitize_recv_opts_for_dataset_type()."""

    def test_rejected_property_mappings_match_zfs_receive_exit_codes(self) -> None:
        """Verifies direct `zfs receive -o/-x` exit codes against the rejected-property mappings."""
        if not is_zfs_at_least_2_1_0():
            self.skipTest("Certain zfs properties require ZFS >= 2.1.0")

        src_filesystem, src_volume = self._create_probe_sources()
        for recv_flag, propname in self._iter_rejected_pairs():
            for dataset_type in _DATASET_TYPES:
                for stream_kind in _STREAM_KINDS:
                    with self.subTest(
                        recv_flag=recv_flag,
                        propname=propname,
                        dataset_type=dataset_type,
                        stream_kind=stream_kind,
                    ):
                        actual_exit_code = self._probe_case(
                            propname=propname,
                            recv_flag=recv_flag,
                            dataset_type=dataset_type,
                            stream_kind=stream_kind,
                            src_filesystem=src_filesystem,
                            src_volume=src_volume,
                        )
                        expected_exit_code = self._expected_exit_code(
                            recv_flag=recv_flag,
                            propname=propname,
                            dataset_type=dataset_type,
                        )
                        self.assertEqual(expected_exit_code, actual_exit_code)

    def test_print_recv_property_buckets(self) -> None:
        """Prints sorted property results with bucket recommendations derived from actual ZFS exit codes."""
        if not is_zfs_at_least_2_1_0():
            self.skipTest("Certain zfs properties require ZFS >= 2.1.0")

        src_filesystem, src_volume = self._create_probe_sources()
        results: list[_ProbeResult] = []
        for propname in _ALL_RECV_PROPS:
            for recv_flag in _RECV_FLAGS:
                for dataset_type in _DATASET_TYPES:
                    for stream_kind in _STREAM_KINDS:
                        results.append(
                            _ProbeResult(
                                propname=propname,
                                recv_flag=recv_flag,
                                dataset_type=dataset_type,
                                stream_kind=stream_kind,
                                exit_code=self._probe_case(
                                    propname=propname,
                                    recv_flag=recv_flag,
                                    dataset_type=dataset_type,
                                    stream_kind=stream_kind,
                                    src_filesystem=src_filesystem,
                                    src_volume=src_volume,
                                ),
                            )
                        )

        expected_count = len(_ALL_RECV_PROPS) * len(_RECV_FLAGS) * len(_DATASET_TYPES) * len(_STREAM_KINDS)
        self.assertEqual(expected_count, len(results))
        print("sanitize_recv_opts_for_dataset_type results:")
        for line in self._render_report_lines(results):
            print(line)

    def _create_probe_sources(self) -> tuple[str, str]:
        src_base = create_filesystem(ibase.SRC_ROOT_DATASET, "sanitize_recv_opts_probe")
        src_filesystem = create_filesystem(src_base, "filesystem")
        src_volume = create_volume(src_base, "volume", size="1M", sparse=True)
        take_snapshot(src_filesystem, "s1")
        take_snapshot(src_volume, "s1")
        zfs_set(names=[src_filesystem, src_volume], properties={"bzfs:delta": "1"})
        take_snapshot(src_filesystem, "s2")
        take_snapshot(src_volume, "s2")
        return src_filesystem, src_volume

    def _probe_case(
        self,
        *,
        propname: str,
        recv_flag: str,
        dataset_type: str,
        stream_kind: str,
        src_filesystem: str,
        src_volume: str,
    ) -> int:
        """Runs `zfs send --props --raw ... | zfs receive -o <name=value>` (for -o)
        or `zfs send --props --raw ... | zfs receive -x <name>` (for -x). Returns zfs CLI exit code."""
        src_dataset = src_volume if dataset_type == "volume" else src_filesystem
        recv_arg = (
            f"{propname}={self._recv_o_value(propname, src_filesystem=src_filesystem, src_volume=src_volume)}"
            if recv_flag == "-o"
            else propname
        )
        parent = create_filesystem(
            ibase.DST_ROOT_DATASET,
            f"sanitize_recv_opts_probe/{dataset_type}/{_RECV_FLAG_PATH_TOKENS[recv_flag]}/{propname}/{stream_kind}",
        )
        target = f"{parent}/target"
        try:
            if stream_kind == "incremental":
                self._run_receive_pipeline(
                    send_argv=self._full_send_argv(src_dataset, "s1"), recv_argv=self._recv_argv(target)
                )
                send_argv = self._incremental_send_argv(src_dataset, "s1", "s2")
            else:
                send_argv = self._full_send_argv(src_dataset, "s2")
            recv_argv = self._recv_argv(target, recv_flag=recv_flag, recv_arg=recv_arg)
            return self._run_receive_pipeline(send_argv=send_argv, recv_argv=recv_argv, check=False).returncode
        finally:
            self._destroy_with_retries(parent)

    def _render_report_lines(self, results: list[_ProbeResult]) -> list[str]:
        result_map = {(r.propname, r.recv_flag, r.dataset_type, r.stream_kind): r.exit_code for r in results}
        lines: list[str] = []
        for propname in _ALL_RECV_PROPS:
            fields = [propname]
            for recv_flag in _RECV_FLAGS:
                filesystem_codes = (
                    result_map[(propname, recv_flag, "filesystem", _STREAM_KINDS[0])],
                    result_map[(propname, recv_flag, "filesystem", _STREAM_KINDS[1])],
                )
                volume_codes = (
                    result_map[(propname, recv_flag, "volume", _STREAM_KINDS[0])],
                    result_map[(propname, recv_flag, "volume", _STREAM_KINDS[1])],
                )
                bucket = self._recommend_bucket(recv_flag, filesystem_codes=filesystem_codes, volume_codes=volume_codes)
                fields.append(f"{recv_flag}={bucket} fs={filesystem_codes} vol={volume_codes}")
            lines.append(" | ".join(fields))
        return lines

    def _recommend_bucket(
        self,
        recv_flag: str,
        *,
        filesystem_codes: tuple[int, int],
        volume_codes: tuple[int, int],
    ) -> str:
        if all(code == 0 for code in filesystem_codes + volume_codes):
            return "-"
        if all(code != 0 for code in filesystem_codes) and all(code == 0 for code in volume_codes):
            return _BUCKET_NAMES[recv_flag]["volume_only"]
        if all(code == 0 for code in filesystem_codes) and all(code != 0 for code in volume_codes):
            return _BUCKET_NAMES[recv_flag]["filesystem_only"]
        return f"mixed(fs={filesystem_codes},vol={volume_codes})"

    def _expected_exit_code(self, *, recv_flag: str, propname: str, dataset_type: str) -> int:
        rejected_props = _RECV_PROP_BUCKETS[dataset_type][recv_flag]
        if propname in rejected_props:
            return _EXPECTED_REJECT_EXIT_CODE
        return _EXPECTED_ACCEPT_EXIT_CODE

    def _recv_o_value(self, propname: str, *, src_filesystem: str, src_volume: str) -> str:
        if propname in _RECV_O_VALUE_OVERRIDES:
            return _RECV_O_VALUE_OVERRIDES[propname]
        for dataset, dataset_type in ((src_filesystem, "filesystem"), (src_volume, "volume")):
            value = zfs_get(
                names=[dataset],
                props=[propname],
                types=[dataset_type],
                max_depth=0,
                fields=["value"],
                splitlines=False,
            )
            assert isinstance(value, str)
            if value != "-":
                return value
        self.fail(f"Failed to determine a valid value for property: {propname}")
        raise AssertionError("unreachable")

    def _full_send_argv(self, dataset: str, snapshot_tag: str) -> list[str]:
        return SUDO_CMD + ["zfs", "send", "--props", "--raw", f"{dataset}@{snapshot_tag}"]

    def _incremental_send_argv(self, dataset: str, from_snapshot_tag: str, to_snapshot_tag: str) -> list[str]:
        return SUDO_CMD + [
            "zfs",
            "send",
            "--props",
            "--raw",
            "-i",
            f"{dataset}@{from_snapshot_tag}",
            f"{dataset}@{to_snapshot_tag}",
        ]

    def _recv_argv(self, target: str, recv_flag: str | None = None, recv_arg: str | None = None) -> list[str]:
        recv_argv = SUDO_CMD + ["zfs", "receive", "-u"]
        if recv_flag is not None:
            assert recv_arg is not None
            recv_argv += [recv_flag, recv_arg]
        recv_argv.append(target)
        return recv_argv

    def _run_receive_pipeline(
        self,
        *,
        send_argv: list[str],
        recv_argv: list[str],
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        cmd = f"set -o pipefail; {shlex.join(send_argv)} | {shlex.join(recv_argv)}"
        process = subprocess.run(
            ["bash", "-lc", cmd],
            check=False,
            text=True,
            capture_output=True,
        )
        if check:
            self.assertEqual(
                0,
                process.returncode,
                msg=(
                    f"command: {cmd}\n"
                    f"exit={process.returncode}\n"
                    f"stdout:\n{process.stdout}\n\n"
                    f"stderr:\n{process.stderr}"
                ),
            )
        return process

    @staticmethod
    def _iter_rejected_pairs() -> tuple[tuple[str, str], ...]:
        return tuple(
            sorted(
                {
                    (recv_flag, propname)
                    for buckets in (_ZFS_RECV_PROPS_REJECTED_ON_FILESYSTEM, _ZFS_RECV_PROPS_REJECTED_ON_ZVOL)
                    for recv_flag, propnames in buckets.items()
                    for propname in propnames
                }
            )
        )

    def _destroy_with_retries(self, dataset: str) -> None:
        for _ in range(50):
            if not dataset_exists(dataset):
                return
            process = subprocess.run(
                SUDO_CMD + ["zfs", "destroy", "-r", "-f", dataset], check=False, text=True, capture_output=True
            )
            if process.returncode == 0:
                return
            time.sleep(0.1)
        if dataset_exists(dataset):
            destroy(dataset, recursive=True, force=True)
