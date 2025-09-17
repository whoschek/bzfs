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

# Inline script metadata conforming to https://packaging.python.org/specifications/inline-script-metadata
# /// script
# requires-python = ">=3.8"
# dependencies = ["PyYAML==6.0.2"]
# ///
"""YAML-driven job launcher around bzfs_jobrunner.

Purpose:
    Provide a production-ready CLI that reads a YAML configuration file and
    launches `bzfs_jobrunner` with equivalent parameters to what
    `bzfs_tests/bzfs_job_example.py` produces. This caters to users who prefer
    declarative configs over Python jobconfig scripts.

Assumptions:
    - The YAML parser is optional. We try to import PyYAML lazily and fail
      with a clear message if it is not installed. Other parts of bzfs must
      remain dependency-free.
    - The YAML schema mirrors `bzfs_job_example.py` in spirit and supports
      feature-parity for commonly used fields. Advanced or rarely used
      arguments may be passed through via `extra_args` to preserve full power.

Design Rationale:
    - Keep the launcher thin and robust: validate inputs, translate config
      fields to `bzfs_jobrunner` CLI flags, and delegate execution. Avoid
      duplicating jobrunner logic. Forward unknown/advanced flags via
      `extra_args`. Ensure safe to re-run, idempotent flows.
"""

from __future__ import annotations
import argparse
import io
import json
import sys
from typing import Any, Iterable

from bzfs_main.argparse_actions import (
    NonEmptyStringAction,
)
from bzfs_main.argparse_cli import PROG_AUTHOR

PROG_NAME = "bzfs_joblauncher"


def argument_parser() -> argparse.ArgumentParser:
    """Returns the CLI parser used by bzfs_joblauncher.

    Exposes top-level job commands equivalent to `bzfs_jobrunner` and a
    required YAML `--config` file. Most fine-grained options are expected to be
    specified either directly in the YAML or forwarded through `extra_args` in
    the YAML.
    """

    # fmt: off
    parser = argparse.ArgumentParser(
        prog=PROG_NAME,
        allow_abbrev=False,
        formatter_class=argparse.RawTextHelpFormatter,
        description="""
This program is a convenience wrapper around [bzfs_jobrunner](README_bzfs_jobrunner.md) that loads a YAML configuration
file and launches `bzfs_jobrunner` with the corresponding arguments.

It is intended for users who prefer declaring fleet-wide job configuration in YAML instead of writing a Python jobconfig
script. The YAML schema aims for feature parity with what `bzfs_tests/bzfs_job_example.py` facilitates. Advanced or
rarely-used flags can be provided via `extra_args` in the YAML and will be forwarded verbatim to `bzfs_jobrunner`.

Most commonly, you will run this from cron or a systemd timer on source and destination hosts, passing one or more of the
job commands below. See `README_bzfs_jobrunner.md` for operational guidance, and `getting_started_bzfs_joblauncher.md` for
an end-to-end example.
""",
    )

    # Commands (forwarded to jobrunner):
    parser.add_argument(
        "--create-src-snapshots", action="store_true",
        help="Take snapshots on the selected source hosts as necessary. Typically executed on each src host.\n\n",
    )
    parser.add_argument(  # identical behavior to bzfs_jobrunner
        "--replicate", choices=["pull", "push"], default=None, const="pull", nargs="?", metavar="",
        help="Replicate snapshots from selected source hosts to selected destination hosts. Default mode is 'pull' if the"
             " option is given without a value.\n\n",
    )
    parser.add_argument(
        "--prune-src-snapshots", action="store_true",
        help="Prune snapshots on the selected source hosts as necessary. Typically executed on each src host.\n\n",
    )
    parser.add_argument(
        "--prune-src-bookmarks", action="store_true",
        help="Prune bookmarks on the selected source hosts as necessary. Typically executed on each src host.\n\n",
    )
    parser.add_argument(
        "--prune-dst-snapshots", action="store_true",
        help="Prune snapshots on the selected destination hosts as necessary. Typically executed on each dst host.\n\n",
    )
    parser.add_argument(
        "--monitor-src-snapshots", action="store_true",
        help="Alert if snapshots on selected source hosts are too old, using monitor plan from the YAML.\n\n",
    )
    parser.add_argument(
        "--monitor-dst-snapshots", action="store_true",
        help="Alert if snapshots on selected destination hosts are too old, using monitor plan from the YAML.\n\n",
    )

    # Options for subsetting hosts from the YAML (forwarded to jobrunner):
    parser.add_argument(
        "--src-host", default=None, action="append", metavar="STRING",
        help="For subsetting src hosts defined in YAML; can be specified multiple times.\n\n",
    )
    parser.add_argument(
        "--dst-host", default=None, action="append", metavar="STRING",
        help="For subsetting destination hosts defined in YAML; can be specified multiple times.\n\n",
    )

    # YAML config:
    parser.add_argument(
        "--config", required=True, action=NonEmptyStringAction, metavar="FILE",
        help="Path to YAML config file. See bzfs_tests/bzfs_joblauncher_example.yaml for structure and documentation.\n\n",
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s (author: {PROG_AUTHOR})",
        help="Show program's version number and exit.\n\n",
    )
    return parser
    # fmt: on


def _read_yaml(path: str) -> dict[str, Any]:
    """Loads and returns YAML content.

    Imports the YAML parser lazily to keep it optional for users not using the joblauncher. Emits a helpful message if the
    parser is not available.
    """

    try:
        import yaml
    except Exception as e:  # pragma: no cover - exact ImportError text varies across environments
        print(
            "ERROR: PyYAML is required for bzfs_joblauncher. Install with: pip install 'bzfs[joblauncher]'",
            file=sys.stderr,
        )
        raise SystemExit(2) from e

    try:
        with open(path, "r", encoding="utf-8") as fd:
            data = yaml.safe_load(fd) or {}
            if not isinstance(data, dict):
                raise TypeError("Top-level YAML content must be a mapping (dictionary)")
            return data
    except OSError as e:
        print(f"ERROR: Cannot read YAML file: {e}", file=sys.stderr)
        raise SystemExit(2) from e
    except Exception as e:
        print(f"ERROR: Failed parsing YAML: {e}", file=sys.stderr)
        raise SystemExit(2) from e


def _as_cli_literal(obj: Any) -> str:
    """Returns a Python-literal-style string suitable for jobrunner's literal_eval.

    We deliberately use json.dumps for deterministic, safe quoting, then lean on
    Python-literal compatibility: lists/dicts of strings/ints/bools/null are
    compatible. This avoids exotic Python objects and ensures robust passing via
    CLI arguments.
    """

    return json.dumps(obj, separators=(",", ":"))


def _normalize_root_dataset_pairs(pairs: Any) -> list[str]:
    """Converts root_dataset_pairs to a flat alternating SRC, DST list.

    Supported YAML forms:
      - sequence of mappings: [{src: "tank1/foo", dst: "tank2/foo"}, ...]
      - sequence of 2-item sequences: [["tank1/foo", "tank2/foo"], ...]
      - flat alternating list: ["tank1/foo", "tank2/foo", ...]
    """

    if isinstance(pairs, list):
        if all(isinstance(x, dict) and "src" in x and "dst" in x for x in pairs):
            out: list[str] = []
            for item in pairs:
                out.extend([str(item["src"]), str(item["dst"])])
            return out
        if all(isinstance(x, (list, tuple)) and len(x) == 2 for x in pairs):
            out = []
            for a, b in pairs:
                out.extend([str(a), str(b)])
            return out
        if all(isinstance(x, str) for x in pairs):
            if len(pairs) % 2 != 0:
                raise ValueError("root_dataset_pairs: Each SRC must have a DST; need an even number of strings")
            return [str(x) for x in pairs]
    raise ValueError(
        "root_dataset_pairs must be a list of {src,dst} mappings, a list of 2-item lists/tuples, or a flat list of strings"
    )


def _get_bool(d: dict[str, Any], key: str, default: bool = False) -> bool:
    v = d.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in {"1", "true", "yes", "on"}
    if isinstance(v, (int, float)):
        return bool(v)
    return default


def _extend_if_present(args: list[str], flag: str, value: Any, *, to_literal: bool = False) -> None:
    if value is None:
        return
    if to_literal:
        args.append(f"{flag}={_as_cli_literal(value)}")
    else:
        args.append(f"{flag}={value}")


def _extend_list(args: list[str], flag: str, seq: Iterable[str]) -> None:
    for x in seq:
        args.append(flag)
        args.append(x)


def main() -> None:
    """CLI entry point; reads YAML, builds command, and runs jobrunner."""

    args, unknown = argument_parser().parse_known_args()

    # 1) Load YAML config
    cfg = _read_yaml(args.config)

    # 3) Collect common settings from YAML
    try:
        src_hosts = cfg["src_hosts"]
        dst_hosts = cfg["dst_hosts"]
        retain_dst_targets = cfg.get("retain_dst_targets", dst_hosts)
        dst_root_datasets = cfg["dst_root_datasets"]

        # Plans
        src_snapshot_plan = cfg["src_snapshot_plan"]
        src_bookmark_plan = cfg.get("src_bookmark_plan", src_snapshot_plan)
        if "dst_snapshot_plan" in cfg:
            dst_snapshot_plan = cfg["dst_snapshot_plan"]
        else:
            mult = cfg.get("dst_snapshot_plan_multiplier")
            if mult is None:
                dst_snapshot_plan = src_bookmark_plan
            else:
                # multiply values by factor (int or float)
                def _mul(x: Any) -> Any:
                    if isinstance(x, (int, float)):
                        return round(float(x) * float(mult))
                    return x

                dst_snapshot_plan = {
                    org: {target: {period: _mul(n) for period, n in periods.items()} for target, periods in targets.items()}
                    for org, targets in src_snapshot_plan.items()
                }

        monitor_snapshot_plan = cfg.get("monitor_snapshot_plan", {})

        # Core execution parameters
        recursive = _get_bool(cfg, "recursive", True)
        workers = cfg.get("workers", "100%")  # int or string like "100%"
        work_period_seconds = int(cfg.get("work_period_seconds", 0))
        jitter = _get_bool(cfg, "jitter", False)
        worker_timeout_seconds = cfg.get("worker_timeout_seconds")
        job_id = cfg.get("job_id")
        log_dir = cfg.get("log_dir")
        bzfs_extra_args: list[str] = list(map(str, (cfg.get("extra_args") or [])))

        # dataset pairs
        root_dataset_pairs = _normalize_root_dataset_pairs(cfg["root_dataset_pairs"])  # raises on error
    except KeyError as e:
        print(f"ERROR: Missing required YAML key: {e}", file=sys.stderr)
        raise SystemExit(2) from e
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(2) from e

    # 4) Build jobrunner command
    cmd: list[str] = ["bzfs_jobrunner"]
    if recursive:
        cmd.append("--recursive")
    _extend_if_present(cmd, "--dst-hosts", dst_hosts, to_literal=True)
    _extend_if_present(cmd, "--retain-dst-targets", retain_dst_targets, to_literal=True)
    _extend_if_present(cmd, "--dst-root-datasets", dst_root_datasets, to_literal=True)
    _extend_if_present(cmd, "--src-snapshot-plan", src_snapshot_plan, to_literal=True)
    _extend_if_present(cmd, "--src-bookmark-plan", src_bookmark_plan, to_literal=True)
    _extend_if_present(cmd, "--dst-snapshot-plan", dst_snapshot_plan, to_literal=True)
    _extend_if_present(cmd, "--monitor-snapshot-plan", monitor_snapshot_plan, to_literal=True)
    _extend_if_present(cmd, "--workers", workers)
    _extend_if_present(cmd, "--work-period-seconds", work_period_seconds)
    if jitter:
        cmd.append("--jitter")
    if worker_timeout_seconds is not None:
        cmd.append(f"--worker-timeout-seconds={int(worker_timeout_seconds)}")
    if job_id:
        cmd.append(f"--job-id={job_id}")
    if log_dir:
        cmd.append(f"--log-dir={log_dir}")

    # Forward launcher CLI commands and host subset flags
    any_cli_action = bool(
        args.create_src_snapshots
        or (args.replicate is not None)
        or args.prune_src_snapshots
        or args.prune_src_bookmarks
        or args.prune_dst_snapshots
        or args.monitor_src_snapshots
        or args.monitor_dst_snapshots
    )

    def _apply_actions_from_yaml(commands: Any) -> None:
        """Optionally populate actions from YAML when CLI did not specify any.

        Accepts either a list of strings (e.g., ["create-src-snapshots", "replicate"]) or a mapping like {"replicate":
        "pull", "create-src-snapshots": true} to specify mode for replicate.
        """

        if not commands:
            return
        replicate_mode: str | None = None
        if isinstance(commands, dict):
            # Mapping: keys may include 'replicate' with value 'pull'/'push' (or truthy), and booleans for others
            if commands.get("create-src-snapshots"):
                cmd.append("--create-src-snapshots")
            if commands.get("replicate") is not None:
                val = commands.get("replicate")
                replicate_mode = str(val) if isinstance(val, str) else None
                cmd.append("--replicate")
                if replicate_mode in ("pull", "push"):
                    cmd.append(replicate_mode)
            if commands.get("prune-src-snapshots"):
                cmd.append("--prune-src-snapshots")
            if commands.get("prune-src-bookmarks"):
                cmd.append("--prune-src-bookmarks")
            if commands.get("prune-dst-snapshots"):
                cmd.append("--prune-dst-snapshots")
            if commands.get("monitor-src-snapshots"):
                cmd.append("--monitor-src-snapshots")
            if commands.get("monitor-dst-snapshots"):
                cmd.append("--monitor-dst-snapshots")
        elif isinstance(commands, list):
            # Simple list of action names; replicate defaults to pull when no mode provided
            if "create-src-snapshots" in commands:
                cmd.append("--create-src-snapshots")
            if "replicate" in commands:
                cmd.append("--replicate")  # default to pull per jobrunner
            if "prune-src-snapshots" in commands:
                cmd.append("--prune-src-snapshots")
            if "prune-src-bookmarks" in commands:
                cmd.append("--prune-src-bookmarks")
            if "prune-dst-snapshots" in commands:
                cmd.append("--prune-dst-snapshots")
            if "monitor-src-snapshots" in commands:
                cmd.append("--monitor-src-snapshots")
            if "monitor-dst-snapshots" in commands:
                cmd.append("--monitor-dst-snapshots")

    if not any_cli_action:
        _apply_actions_from_yaml(cfg.get("commands"))

    if args.create_src_snapshots:
        cmd.append("--create-src-snapshots")
    if args.replicate is not None:
        if args.replicate:
            cmd.append("--replicate")
            if args.replicate in ("pull", "push"):
                cmd.append(args.replicate)
    if args.prune_src_snapshots:
        cmd.append("--prune-src-snapshots")
    if args.prune_src_bookmarks:
        cmd.append("--prune-src-bookmarks")
    if args.prune_dst_snapshots:
        cmd.append("--prune-dst-snapshots")
    if args.monitor_src_snapshots:
        cmd.append("--monitor-src-snapshots")
    if args.monitor_dst_snapshots:
        cmd.append("--monitor-dst-snapshots")

    if args.src_host:
        _extend_list(cmd, "--src-host", args.src_host)
    if args.dst_host:
        _extend_list(cmd, "--dst-host", args.dst_host)

    # Passthrough extra args from YAML and from our own CLI (unknown)
    cmd += bzfs_extra_args + unknown

    # root dataset pairs at the end as alternating SRC DST
    cmd.append("--root-dataset-pairs")
    cmd += root_dataset_pairs

    # 5) Execute jobrunner in-process with src_hosts provided via stdin
    from bzfs_main import bzfs_jobrunner

    input_str = _as_cli_literal(src_hosts)
    old_stdin = sys.stdin
    sys.stdin = io.StringIO(input_str)
    rc = 0
    try:
        bzfs_jobrunner.Job().run_main(cmd)
    except SystemExit as e:
        rc = 0 if (e.code is None) else int(e.code)
    except BaseException as e:  # pragma: no cover - defensive safety net
        print(f"ERROR: Unexpected failure invoking bzfs_jobrunner: {e}", file=sys.stderr)
        rc = 1
    finally:
        sys.stdin = old_stdin
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
