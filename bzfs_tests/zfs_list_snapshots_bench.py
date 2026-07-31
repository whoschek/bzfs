#!/usr/bin/env python3

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
"""Benchmark `zfs list -t snapshot` and `zfs list -t bookmark` commands, optionally in parallel."""

from __future__ import (
    annotations,
)
import argparse
import logging
import os
import platform
import resource
import shutil
import socket
import statistics
import subprocess
import sys
import time
from collections import (
    defaultdict,
)
from collections.abc import (
    Sequence,
)
from dataclasses import (
    asdict,
    dataclass,
)
from datetime import (
    datetime,
)
from pathlib import (
    Path,
)
from typing import (
    Final,
    final,
)

_MOUNTPOINT_ROOT: Final = Path("/mnt") / Path(__file__).stem
_MODES: Final = ("fs-mounted", "fs-unmounted", "zvol")
_LOGGER: Final = logging.getLogger(__name__)


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        allow_abbrev=False,
        prog=Path(__file__).name,
        formatter_class=argparse.RawTextHelpFormatter,
        description="""Benchmark `zfs list -t snapshot` and `zfs list -t bookmark` commands, optionally in parallel.

The benchmark harness has zero dependencies beyond the Python (>= 3.9) standard library.

## Workload

The benchmark executes commands like this:

```shell
printf 'bzfs-perf/zfs_list_snapshots_bench/fs-mounted/fs%05d\\n' {0..7} |
    time xargs -r -n1 -P4 \\
        zfs list -H -p -t snapshot,bookmark -d 1 -o createtxg,guid,name,creation,userrefs,type >/dev/null
```

Each datapoint lists all configured snapshots and/or bookmarks across every dataset of one selected workload type, using one
selected `xargs` process count. The number of warmup and measured trials is configurable.

Benchmark datasets are grouped by workload below the ZFS filesystem selected by `--root-dataset`:

```text
ROOT_DATASET/
├── fs-mounted/fs00000 ... fsNNNNN
├── fs-unmounted/fs00000 ... fsNNNNN
└── zvols/zvol00000 ... zvolNNNNN
```

## Modes

- `fs-mounted`: ZFS filesystems that remain mounted.
- `fs-unmounted`: separate ZFS filesystems that remain unmounted with `mountpoint=none`.
- `zvol`: ZFS volumes with `volmode=none`.

Setup establishes each workload's permanent state, and validation checks that mounted and unmounted datasets remain in their
expected states before a run.

## Commands

- `setup`: recursively destroy the configured --root-dataset if it exists, recreate all requested datasets and workloads,
  then validate them.
- `run`: validate and benchmark the selected mode/nprocs matrix, then write results.

## Results

Every run creates a timestamped directory (below --results) containing:

- `raw.tsv`: warmup and measured wall, user, and system timings.
- `summary.tsv`: elapsed-time and objects/second minimum, maximum, median, average, and standard deviation.
- `summary.md`: a readable result table.
- `config.env`: the exact workload and selected matrix.
- `metadata.env`: VM, kernel, ZFS, ARC, and Git revisions.

""",
    )
    parser.add_argument(
        "command",
        choices=("setup", "run"),
        help="Operation to perform: setup creates and validates the workload; run validates and benchmarks it.\n\n",
    )
    parser.add_argument(
        "--root-dataset",
        default="bzfs-perf/" + Path(__file__).stem,
        metavar="STRING",
        help=(
            "ZFS filesystem that contains all benchmark datasets. Setup recursively destroys this exact dataset and "
            "all descendants if it exists, then recreates the requested workload. (default: %(default)s)\n\n"
        ),
    )
    parser.add_argument(
        "--dataset-count",
        default=8,
        type=int,
        metavar="INT",
        help=(
            "Number of datasets of each selected type that the setup command creates. Each benchmark trial lists "
            "this many filesystems or zvols. (default: %(default)s)\n\n"
        ),
    )
    parser.add_argument(
        "--snapshots-per-dataset",
        default=2000,
        type=int,
        metavar="INT",
        help=(
            "Number (N) of selected objects that setup creates per benchmark dataset. Snapshot mode creates N "
            "snapshots; snapshot,bookmark mode creates N of each; bookmark mode creates N bookmarks from one snapshot. "
            "(default: %(default)s)\n\n"
        ),
    )
    type_choices = ("snapshot", "snapshot,bookmark", "bookmark")
    parser.add_argument(
        "--create-type",
        choices=type_choices,
        default="snapshot,bookmark",
        metavar="TYPE",
        help=(
            "ZFS object types that setup creates. Bookmark-only workloads create one source snapshot. "
            "(default: %(default)s)\n\n"
        ),
    )
    parser.add_argument(
        "--list-type",
        choices=type_choices,
        default="snapshot",
        metavar="TYPE",
        help=(
            "ZFS object types passed to `zfs list -t`. Every selected type must be present in the current datasets, "
            "but additional unlisted types are allowed. (default: %(default)s)\n\n"
        ),
    )
    parser.add_argument(
        "--modes",
        default=",".join(_MODES),
        metavar="STRINGS",
        help=(
            "Select workload states: fs-mounted (mounted ZFS filesystems), fs-unmounted (unmounted ZFS filesystems), "
            "or zvol (volmode=none). Separate multiple modes with commas. "
            "(default: %(default)s)\n\n"
        ),
    )
    parser.add_argument(
        "--nprocs",
        default="1,2,4,8",
        metavar="INTS",
        help=(
            "Number of concurrent processes to benchmark. Each process runs one non-recursive `zfs list` for "
            "one dataset. Separate multiple positive integers with commas. (default: %(default)s)\n\n"
        ),
    )
    parser.add_argument(
        "--columns",
        default="createtxg,guid,name,creation,userrefs,type",
        metavar="STRINGS",
        help=(
            "Comma-separated ZFS properties passed to zfs list -o. Output is discarded, but the selected "
            "properties can affect listing performance. (default: %(default)s)\n\n"
        ),
    )
    parser.add_argument(
        "--sort-columns",
        default="",
        metavar="STRINGS",
        help=(
            "Comma-separated ZFS properties passed to zfs list -s. Output is discarded, but the selected "
            "properties can affect listing performance. (default is no sort)\n\n"
        ),
    )
    parser.add_argument(
        "--zfs_snapshot_list_batch_time_us",
        default=10000,
        type=int,
        metavar="INT",
        help=(
            "zfs kernel module parameter. Set this to 0 on a system that does not support this tuning knob. "
            "(default: %(default)s)\n\n"
        ),
    )
    parser.add_argument(
        "--zfs_snapshot_list_batch_size",
        default=1024,
        type=int,
        metavar="INT",
        help=(
            "zfs kernel module parameter. Set this to 0 on a system that does not support this tuning knob. "
            "(default: %(default)s)\n\n"
        ),
    )
    parser.add_argument(
        "--warmup-trials",
        default=1,
        type=int,
        metavar="INT",
        help=(
            "Number of timed warmup trials before measurements for each mode and nprocs combination. "
            "Recorded in raw.tsv but excluded from summary statistics. (default: %(default)s)\n\n"
        ),
    )
    parser.add_argument(
        "--measurement-trials",
        default=5,
        type=int,
        metavar="INT",
        help=(
            "Number of measured trials for each mode and nprocs combination. Their elapsed times and per-trial "
            "objects/second determine the reported minimum, maximum, median, average, and sample standard deviation. "
            "A single measured trial has zero standard deviation. (default: %(default)s)\n\n"
        ),
    )
    parser.add_argument(
        "--results",
        default=Path("results"),
        type=Path,
        metavar="DIR",
        help=(
            "The run command writes raw timings, summaries, configuration, and system metadata beneath a timestamped "
            "subdirectory. (default: %(default)s)\n\n"
        ),
    )
    parser.add_argument(
        "--label",
        default="baseline",
        metavar="STRING",
        help="Identifier appended to the timestamped results-directory name. (default: %(default)s)\n\n",
    )
    parser.add_argument(
        "--no-create-recursive",
        action="store_true",
        help=(
            "Create snapshots without passing `-r` to `zfs snapshot`, i.e. non-atomically. The same number of snapshots "
            "are created regardless of this option.\n\n"
        ),
    )
    parser.add_argument(
        "--zvol-size",
        default=64,
        type=int,
        metavar="INT",
        help=(
            "Logical size in MB assigned to each sparse zvol that the setup command creates. Used only by the zvol "
            "mode. (default: %(default)s)\n\n"
        ),
    )
    return parser


def main(argv: Sequence[str]) -> int:
    """API for command-line clients."""
    logging.basicConfig(
        datefmt="%Y-%m-%d %H:%M:%S", format="[%(asctime)s] %(message)s", level=logging.INFO, stream=sys.stdout
    )
    command, config = _parse_args(argv)
    try:
        benchmark = _Benchmark(config)
        if command == "setup":
            benchmark.setup()
        else:
            benchmark.run_benchmark()
    except RuntimeError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as error:
        detail = error.stderr.strip() if error.stderr else ""
        suffix = f": {detail}" if detail else ""
        print(f"ERROR: command failed with exit code {error.returncode}{suffix}", file=sys.stderr)
        return error.returncode or 1
    except OSError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


@final
@dataclass(frozen=True)
class _Config:
    """Validated immutable options."""

    root_dataset: str
    dataset_count: int
    snapshots_per_dataset: int
    create_type: str
    list_type: str
    no_create_recursive: bool
    zvol_size: int
    nprocs: tuple[int, ...]
    warmup_trials: int
    measurement_trials: int
    columns: str
    sort_columns: str
    zfs_snapshot_list_batch_time_us: int
    zfs_snapshot_list_batch_size: int
    modes: tuple[str, ...]
    run_label: str
    results_root: Path


@final
@dataclass(frozen=True)
class _SummaryStats:
    """Basic descriptive statistics of one sample."""

    count: int
    minimum: float
    maximum: float
    median: float
    mean: float
    stddev: float

    @classmethod
    def from_values(cls, values: Sequence[float]) -> _SummaryStats:
        return _SummaryStats(
            count=len(values),
            minimum=min(values),
            maximum=max(values),
            median=statistics.median(values),
            mean=statistics.mean(values),
            stddev=statistics.stdev(values) if len(values) > 1 else 0.0,
        )


@final
class _Benchmark:
    """Create, validate, and run workloads."""

    def __init__(self, config: _Config) -> None:
        self._config: _Config = config
        self._zfs: str = _resolve_program("zfs")
        self._sudo: str = _resolve_program("sudo") if os.geteuid() != 0 else ""
        self._xargs: str = _resolve_program("xargs")
        self._tee: str = _resolve_program("tee")

    def setup(self) -> None:
        """Recreate all requested datasets, populate them, then validate their final state."""
        config = self._config
        self._setup_root()
        datasets: list[str] = []
        for mode in config.modes:
            if mode.startswith("fs-"):
                datasets += self._setup_filesystems(mode)
        if "zvol" in config.modes:
            datasets += self._setup_zvols()
        self._setup_objects(datasets)
        self.validate()

    def validate(self) -> tuple[dict[str, list[str]], list[str]]:
        """Validate requested datasets and return their ordered dataset names."""
        config = self._config
        root = config.root_dataset
        if not self._zfs_exists(root):
            raise RuntimeError(f"Missing root filesystem: {root}; run setup")
        dataset_type = self._output([self._zfs, "list", "-H", "-o", "type", root])
        if dataset_type != "filesystem":
            raise RuntimeError(f"Root dataset is not a filesystem: {root}")
        filesystems = {mode: self._validate_filesystems(mode) for mode in config.modes if mode.startswith("fs-")}
        zvols: list[str] = []
        if "zvol" in config.modes:
            expected = self._zvol_names()
            zvols = self._zfs_names("volume", self._zvol_root())
            self._validate_names("zvol", expected, zvols)
            for dataset in zvols:
                self._validate_object_counts(dataset)
        _log("Validated requested benchmark workloads")
        return filesystems, zvols

    def run_benchmark(self) -> None:
        """Run the selected matrix and write raw and summarized results."""
        if self._config.zfs_snapshot_list_batch_time_us > 0:
            self._set_zfs_module_param("zfs_snapshot_list_batch_time_us", str(self._config.zfs_snapshot_list_batch_time_us))
        if self._config.zfs_snapshot_list_batch_size > 0:
            self._set_zfs_module_param("zfs_snapshot_list_batch_size", str(self._config.zfs_snapshot_list_batch_size))
        scope = f"modes={','.join(self._config.modes)} with {self._config.dataset_count} datasets per mode"
        _log(f"Validating requested benchmark workloads for {scope} ...")
        filesystems, zvols = self.validate()
        timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        output_dir = self._config.results_root / f"{timestamp}-{self._config.run_label}"
        output_dir.mkdir(parents=True, exist_ok=False)
        self._write_metadata(output_dir)
        raw_file = output_dir / "raw.tsv"
        raw_file.write_text(
            "mode\tnprocs\tphase\ttrial\telapsed_seconds\tuser_seconds\tsystem_seconds\tobject_count\tobjects_per_second\n",
            encoding="utf-8",
        )
        measurements: dict[tuple[str, int], list[float]] = defaultdict(list)
        for mode in self._config.modes:
            datasets = filesystems[mode] if mode.startswith("fs-") else zvols
            for nprocs in self._config.nprocs:
                for phase, trials in [
                    ("warmup", self._config.warmup_trials),
                    ("measurement", self._config.measurement_trials),
                ]:
                    throughputs = []
                    for trial in range(1, trials + 1):
                        elapsed, user_seconds, system_seconds, throughput = self._timed_trial(
                            mode, nprocs, phase, trial, datasets
                        )
                        throughputs.append(throughput)
                        object_count = _listed_object_count(self._config)
                        with raw_file.open("a", encoding="utf-8") as stream:
                            stream.write(
                                f"{mode}\t{nprocs}\t{phase}\t{trial}\t{elapsed:.6f}\t"
                                f"{user_seconds:.6f}\t{system_seconds:.6f}\t{object_count}\t"
                                f"{throughput:.3f}\n"
                            )
                        if phase == "measurement":
                            measurements[(mode, nprocs)].append(elapsed)
                            if trial == trials:
                                _log(
                                    " ".join(
                                        [
                                            f"mode={mode}",
                                            f"nprocs={nprocs}",
                                            f"phase={phase}",
                                            f"trial={trial}",
                                            f"elapsed={elapsed:.3f}s",
                                            f"objects/s={statistics.median(throughputs):.3f} median",
                                        ]
                                    )
                                )

        self._write_summary(output_dir, dict(measurements))
        _log(f"Results: {output_dir}")

    def _timed_trial(
        self, mode: str, nprocs: int, phase: str, trial: int, datasets: Sequence[str]
    ) -> tuple[float, float, float, float]:
        datasets_spec = "\n".join(datasets) + "\n"
        sort_spec = (
            [argument for column in self._config.sort_columns.split(",") for argument in ("-s", column.strip())]
            if self._config.sort_columns
            else []
        )
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        start = time.perf_counter()
        subprocess.run(
            [
                self._xargs,
                "-r",
                "-n1",
                f"-P{nprocs}",
                self._zfs,
                "list",
                "-H",
                "-p",
                "-t",
                self._config.list_type,
                "-d",
                "1",
                "-o",
                self._config.columns,
            ]
            + sort_spec,
            input=datasets_spec,
            text=True,
            stdout=subprocess.DEVNULL,
            check=True,
        )
        elapsed = time.perf_counter() - start
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        user_seconds = after.ru_utime - before.ru_utime
        system_seconds = after.ru_stime - before.ru_stime
        object_count = _listed_object_count(self._config)
        throughput = object_count / elapsed
        _log(
            " ".join(
                [
                    f"mode={mode}",
                    f"nprocs={nprocs}",
                    f"phase={phase}",
                    f"trial={trial}",
                    f"elapsed={elapsed:.3f}s",
                    f"objects/s={throughput:.3f}",
                ]
            )
        )
        return elapsed, user_seconds, system_seconds, throughput

    def _set_zfs_module_param(self, name: str, value: str) -> None:
        sudo_cmd = [self._sudo, "-n"] if self._sudo else []
        subprocess.run(
            sudo_cmd + [self._tee, f"/sys/module/zfs/parameters/{name}"],
            text=True,
            check=True,
            stdout=subprocess.DEVNULL,
            input=value + "\n",
        )

    def _setup_root(self) -> None:
        root = self._config.root_dataset
        if self._zfs_exists(root):
            _log(f"Recursively destroying existing root dataset {root}")
            self._run([self._zfs, "destroy", "-r", "-v", root], privileged=True)
        _log(f"Creating root filesystem {root}")
        self._run(
            [self._zfs, "create", "-o", "canmount=off", "-o", "mountpoint=none", root],
            privileged=True,
        )

    def _setup_filesystems(self, mode: str) -> list[str]:
        """Create one mode-specific filesystem tree in its permanent mount state."""
        config = self._config
        filesystem_root = self._filesystem_root(mode)
        _log(f"Creating {config.dataset_count} {mode} filesystem datasets")
        self._run(
            [self._zfs, "create", "-o", "canmount=off", "-o", "mountpoint=none", filesystem_root],
            privileged=True,
        )
        filesystems = self._filesystem_names(mode)
        for filesystem in filesystems:
            mountpoint = "none" if mode == "fs-unmounted" else str(self._filesystem_mountpoint(filesystem))
            self._run([self._zfs, "create", "-o", f"mountpoint={mountpoint}", filesystem], privileged=True)
        return filesystems

    def _setup_zvols(self) -> list[str]:
        """Create the zvol container and its sparse benchmark leaves."""
        config = self._config
        _log(f"Creating {config.dataset_count} sparse {config.zvol_size} MB zvols")
        self._run(
            [self._zfs, "create", "-o", "canmount=off", "-o", "mountpoint=none", self._zvol_root()],
            privileged=True,
        )
        volumes = self._zvol_names()
        for volume in volumes:
            self._run(
                [self._zfs, "create", "-s", "-V", f"{config.zvol_size}M", "-o", "volmode=none", volume],
                privileged=True,
            )
        return volumes

    def _setup_objects(self, datasets: Sequence[str]) -> None:
        config = self._config
        snapshot_count = config.snapshots_per_dataset if "snapshot" in config.create_type else 1
        bookmark_count = config.snapshots_per_dataset if "bookmark" in config.create_type else 0
        for index in range(snapshot_count):
            tag = f"s{index:05d}"
            if config.no_create_recursive:
                for dataset in datasets:
                    self._run([self._zfs, "snapshot", f"{dataset}@{tag}"], privileged=True)
            else:
                self._run([self._zfs, "snapshot", "-r", f"{config.root_dataset}@{tag}"], privileged=True)
            if (index + 1) % 100 == 0:
                _log(f"Created {index + 1} snapshots per dataset")
        for index in range(bookmark_count):
            snapshot_index = index if config.create_type == "snapshot,bookmark" else 0
            for dataset in datasets:
                self._run(
                    [self._zfs, "bookmark", f"{dataset}@s{snapshot_index:05d}", f"{dataset}#b{index:05d}"],
                    privileged=True,
                )
            if (index + 1) % 100 == 0:
                _log(f"Created {index + 1} bookmarks per dataset")

    def _write_summary(self, output_dir: Path, measurements: dict[tuple[str, int], list[float]]) -> None:
        object_count = _listed_object_count(self._config)
        rows: list[tuple[str, int, int, _SummaryStats, _SummaryStats]] = []
        for mode in self._config.modes:
            for nprocs in self._config.nprocs:
                times: list[float] = measurements[(mode, nprocs)]
                objects_per_second = [object_count / elapsed for elapsed in times]
                rows.append(
                    (
                        mode,
                        nprocs,
                        len(times),
                        _SummaryStats.from_values(objects_per_second),
                        _SummaryStats.from_values(times),
                    )
                )
        summary_lines = [
            "mode\tnprocs\tmeasurements\tmin_objects_per_second\tmax_objects_per_second\t"
            "median_objects_per_second\tavg_objects_per_second\tstddev_objects_per_second\t"
            "min_seconds\tmax_seconds\tmedian_seconds\tavg_seconds\tstddev_seconds"
        ]
        summary_lines.extend(
            f"{mode}\t{nprocs}\t{count}\t{rate.minimum:.3f}\t{rate.maximum:.3f}\t{rate.median:.3f}\t"
            f"{rate.mean:.3f}\t{rate.stddev:.3f}\t"
            f"{elapsed.minimum:.3f}\t{elapsed.maximum:.3f}\t{elapsed.median:.3f}\t"
            f"{elapsed.mean:.3f}\t{elapsed.stddev:.3f}"
            for mode, nprocs, count, rate, elapsed in rows
        )
        (output_dir / "summary.tsv").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

        markdown = [
            "# zfs list benchmark result",
            "",
            f"- Run label: `{self._config.run_label}`",
            (
                f"- Workload: {self._config.dataset_count} datasets x "
                f"{self._config.snapshots_per_dataset} of each `{self._config.list_type}` type = "
                f"{object_count} listed objects per datapoint"
            ),
            f"- Trials: {self._config.warmup_trials} warmup + {self._config.measurement_trials} measured per datapoint",
            f"- Create types: `{self._config.create_type}`",
            f"- List types: `{self._config.list_type}`",
            f"- Columns: `{self._config.columns}`",
            f"- Sort columns: `{self._config.sort_columns}`",
            f"- zfs_snapshot_list_batch_time_us: `{self._config.zfs_snapshot_list_batch_time_us}`",
            f"- zfs_snapshot_list_batch_size `{self._config.zfs_snapshot_list_batch_size}`",
            "",
            "| Mode | nprocs | Min objects/sec | Max objects/sec | Median objects/sec | Avg objects/sec "
            "| Stddev objects/sec | Min seconds | Max seconds | Median seconds | Avg seconds | Stddev seconds |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
        markdown.extend(
            f"| {mode} | P{nprocs} | {rate.minimum:.3f} | {rate.maximum:.3f} | {rate.median:.3f} "
            f"| {rate.mean:.3f} | {rate.stddev:.3f} "
            f"| {elapsed.minimum:.3f} | {elapsed.maximum:.3f} | {elapsed.median:.3f} "
            f"| {elapsed.mean:.3f} | {elapsed.stddev:.3f} |"
            for mode, nprocs, _, rate, elapsed in rows
        )
        markdown.extend(
            [
                "",
                "Raw warmup and measurement timings are in `raw.tsv`; system and ZFS details are in `metadata.env`.",
            ]
        )
        (output_dir / "summary.md").write_text("\n".join(markdown) + "\n", encoding="utf-8")

    def _write_metadata(self, output_dir: Path) -> None:
        os_name = "unknown"
        os_release = Path("/etc/os-release")
        if os_release.is_file():
            for line in os_release.read_text(encoding="utf-8").splitlines():
                if line.startswith("PRETTY_NAME="):
                    os_name = line.split("=", 1)[1].strip().strip('"')
                    break
        metadata = [
            ("started", datetime.now().strftime("%Y-%m-%dT%H:%M:%S")),
            ("hostname", socket.gethostname()),
            ("os", os_name),
            ("kernel", self._optional_output(["uname", "-srvm"]) or platform.platform()),
            ("architecture", platform.machine()),
            ("cpus", str(os.cpu_count() or "unknown")),
            ("zfs_version", self._output([self._zfs, "version"]).splitlines()[0]),
        ]
        modinfo = shutil.which("modinfo")
        if modinfo:
            version = self._optional_output([modinfo, "-F", "version", "zfs"])
            if version:
                metadata.append(("zfs_module_version", version))
        compressed_arc = Path("/sys/module/zfs/parameters/zfs_compressed_arc_enabled")
        if compressed_arc.is_file():
            metadata.append(("zfs_compressed_arc_enabled", compressed_arc.read_text(encoding="utf-8").strip()))
        for name, root in [("zfs_git_head", Path("/zfs"))]:
            if not root.is_dir():
                continue
            revision = self._optional_output(["git", "-C", str(root), "rev-parse", "HEAD"])
            if revision:
                metadata.append((name, revision))
        config_lines = []
        for key, value in asdict(self._config).items():
            if isinstance(value, tuple):
                value = ",".join(map(str, value))
            config_lines.append(f"{key}={value}")
        (output_dir / "metadata.env").write_text("".join(f"{key}={value}\n" for key, value in metadata), encoding="utf-8")
        (output_dir / "config.env").write_text("\n".join(config_lines) + "\n", encoding="utf-8")

    def _run(
        self,
        command: Sequence[str],
        *,
        privileged: bool = False,
        capture: bool = False,
        check: bool = True,
        quiet: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        full_command = list(command)
        if privileged and self._sudo:
            full_command = [self._sudo, "-n", *full_command]
        stdout = subprocess.PIPE if capture else subprocess.DEVNULL if quiet else None
        stderr = subprocess.PIPE if capture else subprocess.DEVNULL if quiet else None
        return subprocess.run(full_command, text=True, stdout=stdout, stderr=stderr, check=check)

    def _output(self, command: Sequence[str]) -> str:
        return self._run(command, capture=True).stdout.strip()

    def _zfs_exists(self, dataset: str) -> bool:
        result = self._run([self._zfs, "list", "-H", "-o", "name", dataset], check=False, quiet=True)
        return result.returncode == 0

    def _zfs_get_value(self, prop: str, dataset: str) -> str:
        return self._output([self._zfs, "get", "-H", "-p", "-o", "value", prop, dataset])

    def _zfs_names(self, dataset_type: str, root: str) -> list[str]:
        output = self._output([self._zfs, "list", "-H", "-r", "-t", dataset_type, "-o", "name", root])
        return sorted(name for name in output.splitlines() if name != root)

    def _filesystem_root(self, mode: str) -> str:
        """Return the dataset that contains one filesystem mode's benchmark leaves."""
        if mode not in ("fs-mounted", "fs-unmounted"):
            raise RuntimeError(f"Unsupported filesystem mode: {mode}")
        return f"{self._config.root_dataset}/{mode}"

    def _filesystem_names(self, mode: str) -> list[str]:
        """Return ordered benchmark leaf names for one filesystem mode."""
        root = self._filesystem_root(mode)
        return sorted(f"{root}/fs{index:05d}" for index in range(self._config.dataset_count))

    def _filesystem_mountpoint(self, dataset: str) -> Path:
        """Map a mounted-mode dataset to its distinct absolute mountpoint."""
        mode, leaf = dataset.rsplit("/", 2)[-2:]
        return _MOUNTPOINT_ROOT / mode / leaf

    def _zvol_names(self) -> list[str]:
        """Return ordered benchmark zvol names below their container dataset."""
        root = self._zvol_root()
        return sorted(f"{root}/zvol{index:05d}" for index in range(self._config.dataset_count))

    def _zvol_root(self) -> str:
        """Return the filesystem dataset that contains all benchmark zvols."""
        return f"{self._config.root_dataset}/zvols"

    def _validate_object_counts(self, dataset: str) -> None:
        expected = self._config.snapshots_per_dataset
        for object_type in self._config.list_type.split(","):
            output = self._output([self._zfs, "list", "-H", "-t", object_type, "-o", "name", dataset])
            count = len(output.splitlines()) if output else 0
            if count != expected:
                raise RuntimeError(f"{dataset} has {count} {object_type}s; expected {expected}")

    def _validate_names(self, description: str, expected: Sequence[str], actual: Sequence[str]) -> None:
        if list(expected) != list(actual):
            raise RuntimeError(f"Unexpected {description} dataset names; expected {list(expected)}, found {list(actual)}")

    def _validate_filesystems(self, mode: str) -> list[str]:
        """Validate one mode-specific tree's names, objects, and permanent mount state."""
        expected = self._filesystem_names(mode)
        datasets = self._zfs_names("filesystem", self._filesystem_root(mode))
        self._validate_names(f"{mode} filesystem", expected, datasets)
        for dataset in datasets:
            self._validate_object_counts(dataset)
            mounted = self._zfs_get_value("mounted", dataset)
            expected_mounted = "yes" if mode == "fs-mounted" else "no"
            if mounted != expected_mounted:
                state = "mounted" if expected_mounted == "yes" else "unmounted"
                raise RuntimeError(f"Filesystem must be {state} for {mode}: {dataset}")
            mountpoint = self._zfs_get_value("mountpoint", dataset)
            expected_mountpoint = str(self._filesystem_mountpoint(dataset)) if mode == "fs-mounted" else "none"
            if mountpoint != expected_mountpoint:
                raise RuntimeError(
                    f"Filesystem must have mountpoint={expected_mountpoint} for {mode}: {dataset}; found {mountpoint}"
                )
        return datasets

    def _optional_output(self, command: Sequence[str]) -> str:
        try:
            output = self._output(command)
        except (OSError, subprocess.CalledProcessError):
            return ""
        return output


def _log(message: str) -> None:
    _LOGGER.info(message)


def _listed_object_count(config: _Config) -> int:
    return config.dataset_count * config.snapshots_per_dataset * len(config.list_type.split(","))


def _parse_args(argv: Sequence[str]) -> tuple[str, _Config]:

    def _selection(raw: str) -> tuple[str, ...]:
        return tuple(value.strip() for value in raw.split(",") if value.strip())

    def _unique(parser: argparse.ArgumentParser, name: str, values: Sequence[object]) -> None:
        if len(values) != len(set(values)):
            parser.error(f"Duplicate {name}")

    def _positive_integer(parser: argparse.ArgumentParser, name: str, raw: str | int) -> int:
        try:
            value = int(raw)
        except ValueError:
            parser.error(f"Invalid {name}: {raw}")
        if value <= 0:
            parser.error(f"Invalid {name}: {raw}")
        return value

    parser = _argument_parser()
    args = parser.parse_args(argv)

    modes = _selection(args.modes)
    nprocs_values = _selection(args.nprocs)
    if not modes:
        parser.error("At least one mode is required")
    if not nprocs_values:
        parser.error("At least one nprocs value is required")
    unknown_modes = [mode for mode in modes if mode not in _MODES]
    if unknown_modes:
        parser.error(f"Unknown mode: {unknown_modes[0]}")
    _unique(parser, "mode", modes)
    nprocs = tuple(_positive_integer(parser, "nprocs value", value) for value in nprocs_values)
    _unique(parser, "nprocs value", nprocs)

    dataset_count = _positive_integer(parser, "dataset count", args.dataset_count)
    snapshots = _positive_integer(parser, "snapshots per dataset", args.snapshots_per_dataset)
    zvol_size = _positive_integer(parser, "zvol size", args.zvol_size)
    warmups = _positive_integer(parser, "warmup trials", args.warmup_trials)
    measurements = _positive_integer(parser, "measurement trials", args.measurement_trials)
    if not args.columns:
        parser.error("--columns must not be empty")
    config = _Config(
        root_dataset=args.root_dataset,
        dataset_count=dataset_count,
        snapshots_per_dataset=snapshots,
        create_type=args.create_type,
        list_type=args.list_type,
        no_create_recursive=args.no_create_recursive,
        zvol_size=zvol_size,
        nprocs=nprocs,
        warmup_trials=warmups,
        measurement_trials=measurements,
        columns=args.columns.strip(),
        sort_columns=args.sort_columns.strip(),
        zfs_snapshot_list_batch_time_us=args.zfs_snapshot_list_batch_time_us,
        zfs_snapshot_list_batch_size=args.zfs_snapshot_list_batch_size,
        modes=modes,
        run_label=args.label,
        results_root=args.results,
    )
    return args.command, config


def _resolve_program(program: str) -> str:
    if "/" in program:
        if os.access(program, os.X_OK):
            return program
    else:
        resolved = shutil.which(program)
        if resolved:
            return resolved
    raise RuntimeError(f"Program not found or not executable: {program}")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
