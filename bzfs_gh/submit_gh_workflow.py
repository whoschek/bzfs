#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import pathlib
import shlex
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import socket

import yaml

POLL_SECS: int = 15
MAX_WAIT_SECS: int = 60 * 60  # one hour
GH_MAX_RETRIES: int = 3
GH_RETRY_DELAY: int = 5
JsonDict = Dict[str, Any]
YamlInputs = Dict[str, Dict[str, Any]]
DefaultValue = Union[str, bool, int, float, None]


def network_available(host: str = "github.com", port: int = 443, timeout: int = 3) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def run(
    cmd: List[str],
    *,
    capture: bool = True,
    check: bool = True,
    timeout: Optional[int] = None,
    return_proc: bool = False,
) -> Union[str, subprocess.CompletedProcess[str]]:
    """Run *cmd* with retries and optional timeout.

    When ``capture`` is ``False`` output is streamed to the terminal.
    When ``check`` is ``False`` the return code is ignored after retries.
    """
    last_err = ""
    for attempt in range(1, GH_MAX_RETRIES + 1):
        proc = subprocess.run(
            cmd,
            capture_output=capture,
            text=True,
            timeout=timeout,
        )
        if proc.returncode == 0 or not check:
            if return_proc:
                return proc
            return proc.stdout.strip() if capture else ""
        last_err = proc.stderr.strip()
        if attempt < GH_MAX_RETRIES:
            time.sleep(GH_RETRY_DELAY)
        else:
            if last_err:
                sys.stderr.write(last_err + "\n")
            if check:
                sys.exit(proc.returncode)
            if return_proc:
                return proc
            return proc.stdout.strip() if capture else ""
    return proc if return_proc else ""


def canonicalise_default(raw: Any, ptype: str) -> DefaultValue:
    if raw is None:
        return False if ptype == "boolean" else ""
    if ptype == "boolean":
        return raw if isinstance(raw, bool) else str(raw).lower() == "true"
    if ptype == "number":
        try:
            num: float = float(raw)
            return int(num) if num.is_integer() else num
        except (ValueError, TypeError):
            return raw
    return raw


def load_inputs(yaml_path: str) -> YamlInputs:
    with open(yaml_path, encoding="utf-8") as fh:
        doc: Any = yaml.safe_load(fh)
    try:
        return doc["workflow_dispatch"]["inputs"]
    except (TypeError, KeyError):
        sys.stderr.write(f"{yaml_path} lacks workflow_dispatch.inputs\n")
        sys.exit(1)


def ensure_network() -> None:
    if not network_available():
        sys.stderr.write("Network appears unavailable\n")
        sys.exit(1)


def _bool_flag(name: str, default: bool) -> Dict[str, Any]:
    slug = name.replace("_", "-")
    if default:
        return {"flags": [f"--no-{slug}"], "action": "store_false"}
    return {"flags": [f"--{slug}"], "action": "store_true"}


def make_parser(inputs: YamlInputs) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Trigger workflow_dispatch and wait for result",
    )
    parser.add_argument("repo")
    parser.add_argument("branch")
    parser.add_argument("workflow_file")
    parser.add_argument("--poll-secs", type=int, default=POLL_SECS, help="Polling interval")
    parser.add_argument("--timeout-secs", type=int, default=MAX_WAIT_SECS, help="Max wait seconds for workflow")
    for key, spec in inputs.items():
        ptype: str = spec.get("type", "string")
        default: DefaultValue = canonicalise_default(spec.get("default"), ptype)
        required: bool = bool(spec.get("required", False))
        desc: str = spec.get("description", "")
        flag = f"--{key.replace('_', '-')}"
        if ptype == "boolean":
            info = _bool_flag(key, bool(default))
            parser.add_argument(*info["flags"], dest=key, action=info["action"], default=default, help=desc)
        elif ptype == "choice":
            parser.add_argument(flag, choices=spec.get("options", []), default=default, required=required, help=desc)
        elif ptype == "number":
            num_type = int if isinstance(default, int) else float
            parser.add_argument(flag, type=num_type, default=default, required=required, help=desc)
        else:
            parser.add_argument(flag, type=str, default=default, required=required, help=desc)
    return parser


def latest_run(repo: str, branch: str, workflow_file: str) -> JsonDict:
    raw = run(
        [
            "gh",
            "run",
            "list",
            "-R",
            repo,
            "--workflow",
            workflow_file,
            "--branch",
            branch,
            "--limit",
            "1",
            "--json",
            "databaseId,status,conclusion,htmlURL",
        ]
    )
    data = json.loads(raw)
    if not data:
        sys.stderr.write("No workflow run found\n")
        sys.exit(2)
    return data[0]


def wait_done(repo: str, run_id: int, poll_secs: int, max_wait_secs: int) -> Tuple[JsonDict, int]:
    """Wait for completion using ``gh run watch`` and return run info and exit code."""
    try:
        proc = run(
            [
                "gh",
                "run",
                "watch",
                str(run_id),
                "-R",
                repo,
                "--interval",
                str(poll_secs),
                "--exit-status",
            ],
            capture=False,
            check=False,
            timeout=max_wait_secs,
            return_proc=True,
        )
        exit_code = proc.returncode
    except subprocess.TimeoutExpired:
        sys.stderr.write("\nTimeout waiting for workflow\n")
        return ({"conclusion": "timed_out", "status": "completed", "htmlURL": "", "logUrl": ""}, 1)

    raw = run(
        [
            "gh",
            "run",
            "view",
            str(run_id),
            "-R",
            repo,
            "--json",
            "status,conclusion,htmlURL,logUrl",
        ]
    )
    return json.loads(raw), exit_code


def main(argv: Optional[List[str]] = None) -> None:
    if argv is None:
        argv = sys.argv[1:]
    if len(argv) < 3:
        sys.stderr.write("Usage: submit_workflow.py REPO BRANCH WORKFLOW_FILE [flags …]\n")
        sys.exit(1)
    ensure_network()
    inputs = load_inputs(argv[2])
    parser = make_parser(inputs)
    args = parser.parse_args(argv)
    fields: List[str] = []
    for name in inputs:
        val: Any = getattr(args, name)
        if isinstance(val, bool):
            val = "true" if val else "false"
        elif isinstance(val, (int, float)):
            if isinstance(val, float) and not math.isfinite(val):
                sys.stderr.write(f"Non-finite number supplied for {name}\n")
                sys.exit(1)
            val = str(val)
        fields.extend(["--field", f"{name}={val}"])
    submit_cmd: List[str] = [
        "gh",
        "workflow",
        "run",
        args.workflow_file,
        "-R",
        args.repo,
        "--ref",
        args.branch,
        *fields,
    ]
    print("→", shlex.join(submit_cmd), file=sys.stderr)
    run(submit_cmd)
    run_id = int(latest_run(args.repo, args.branch, args.workflow_file)["databaseId"])
    final, exit_code = wait_done(args.repo, run_id, args.poll_secs, args.timeout_secs)
    conclusion: str = final.get("conclusion") or "unknown"
    result: JsonDict = {
        "run_id": run_id,
        "conclusion": conclusion,
        "html_url": final["htmlURL"],
        "exit_code": exit_code,
    }
    if conclusion != "success":
        tmp_dir = tempfile.mkdtemp(prefix=f"gh_{run_id}_")
        for attempt in range(1, GH_MAX_RETRIES + 1):
            run(["gh", "run", "download", str(run_id), "-R", args.repo, "--dir", tmp_dir])
            zip_path = next(pathlib.Path(tmp_dir).glob("*.zip"), None)
            if zip_path:
                result["log_archive"] = str(zip_path.resolve())
                break
            if attempt < GH_MAX_RETRIES:
                time.sleep(GH_RETRY_DELAY)
    json.dump(result, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
