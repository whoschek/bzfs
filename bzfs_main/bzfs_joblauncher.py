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
"""Reads a YAML jobconfig and runs ``bzfs_jobrunner`` with the same parameters."""

from __future__ import annotations
import argparse
import subprocess
import sys
from typing import Any

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    yaml = None

from bzfs_main.argparse_cli import PROG_AUTHOR, __version__

PROG_NAME = "bzfs_joblauncher"


def argument_parser() -> argparse.ArgumentParser:
    """Returns the CLI parser used by bzfs_joblauncher."""
    parser = argparse.ArgumentParser(
        prog=PROG_NAME,
        allow_abbrev=False,
        formatter_class=argparse.RawTextHelpFormatter,
        description=(
            "Reads a YAML jobconfig and invokes bzfs_jobrunner with equivalent arguments.\n"
            "Unknown CLI arguments are forwarded to bzfs_jobrunner."
        ),
    )
    parser.add_argument("config", help="Path to YAML configuration file.")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__} by {PROG_AUTHOR}")
    return parser


def _build_cmd(config: dict[str, Any], unknown_args: list[str]) -> tuple[list[str], str]:
    """Construct command line and stdin input for ``bzfs_jobrunner``."""
    src_hosts = config.get("src_hosts", [])
    cmd: list[str] = ["bzfs_jobrunner"]
    if config.get("recursive"):
        cmd.append("--recursive")
    mapping_opts = {
        "dst_hosts": "--dst-hosts",
        "retain_dst_targets": "--retain-dst-targets",
        "dst_root_datasets": "--dst-root-datasets",
        "src_snapshot_plan": "--src-snapshot-plan",
        "src_bookmark_plan": "--src-bookmark-plan",
        "dst_snapshot_plan": "--dst-snapshot-plan",
        "monitor_snapshot_plan": "--monitor-snapshot-plan",
        "workers": "--workers",
        "work_period_seconds": "--work-period-seconds",
    }
    for key, opt in mapping_opts.items():
        if key in config:
            cmd.append(f"{opt}={config[key]}")
    if config.get("jitter"):
        cmd.append("--jitter")
    if (timeout := config.get("worker_timeout_seconds")) is not None:
        cmd.append(f"--worker-timeout-seconds={timeout}")
    cmd += config.get("extra_args", []) + unknown_args
    if "root_dataset_pairs" in config:
        pairs = [item for src, dst in config["root_dataset_pairs"] for item in (src, dst)]
        cmd += ["--root-dataset-pairs"] + pairs
    return cmd, f"{src_hosts}"


def run(argv: list[str] | None = None) -> int:
    """Parse CLI/YAML, run ``bzfs_jobrunner``.

    Purpose: forward joblauncher configs to jobrunner.
    Assumes optional PyYAML is present.
    Design: load YAML, check for command flags, then spawn runner.
    """
    if argv is None:
        argv = sys.argv[1:]
    parser = argument_parser()
    args, unknown = parser.parse_known_args(argv)
    if yaml is None:
        print("ERROR: Install optional dependency 'PyYAML' via `pip install bzfs[bzfs_joblauncher]`.", file=sys.stderr)
        return 5
    try:
        with open(args.config, encoding="utf-8") as fd:
            config = yaml.safe_load(fd)
    except yaml.YAMLError as exc:
        print(f"ERROR: Failed to parse YAML config: {exc}", file=sys.stderr)
        return 5
    cmd, stdin_input = _build_cmd(config, unknown)
    command_flags = {
        "--create-src-snapshots",
        "--replicate",
        "--prune-src-snapshots",
        "--prune-src-bookmarks",
        "--prune-dst-snapshots",
        "--monitor-src-snapshots",
        "--monitor-dst-snapshots",
    }
    if not any(flag in cmd for flag in command_flags):
        print(
            "ERROR: Missing command. Usage: "
            + sys.argv[0]
            + " --create-src-snapshots|--replicate|--prune-src-snapshots|"
            + "--prune-src-bookmarks|--prune-dst-snapshots|--monitor-src-snapshots|--monitor-dst-snapshots",
            file=sys.stderr,
        )
        return 5
    return subprocess.run(cmd, input=stdin_input, text=True).returncode


def main(argv: list[str] | None = None) -> None:
    """API for command line clients."""
    sys.exit(run(argv))


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
