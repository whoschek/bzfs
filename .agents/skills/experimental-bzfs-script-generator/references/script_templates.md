# Script Templates

Use these templates to generate minimal scripts. Keep dry-run enabled unless a human explicitly changes the toggle.
Default to returning both Bash and Python variants unless the user requests only one language. For `bzfs_jobrunner`
templates, align action and host-routing with `bzfs_testbed/bzfs_job_testbed.py`. For `bzfs_jobrunner` dict/list
options, align with `bzfs_job_testbed.py`: pass values as `--flag={value}`. For standard schedule policies, prefer
plan-based convenience flags over manual `--include-snapshot-times-and-ranks` chains.

## Bash Template: One-Off bzfs Task

```bash
#!/usr/bin/env bash
set -euo pipefail

SRC_DATASET="src/foo/bar"
DST_DATASET="dst/boo/bar"
DRYRUN="${DRYRUN:-1}"  # keep 1 for safety

cmd=(bzfs "$SRC_DATASET" "$DST_DATASET" --recursive)

# Add task flags here, for example:
# cmd+=(--skip-replication --compare-snapshot-lists=src+dst+all)
# cmd+=(--create-src-snapshots --create-src-snapshots-plan "{'prod':{'onsite':{'minutely':40,'hourly':36,'daily':31}}}")
# cmd+=(--include-snapshot-plan "{'prod':{'onsite':{'minutely':40,'hourly':36,'daily':31}}}")
# cmd+=(--delete-dst-snapshots --delete-dst-snapshots-except-plan "{'prod':{'onsite':{'minutely':40,'hourly':36,'daily':31}}}")

if [[ "$DRYRUN" == "1" ]]; then
  cmd+=(--dryrun)
fi

printf 'Review command before run:\n'
printf '  %q' "${cmd[@]}"
printf '\n'
"${cmd[@]}"
```

## Python Template: One-Off bzfs Task

```python
#!/usr/bin/env python3
"""Run a single bzfs workflow with dry-run enabled by default."""

from __future__ import annotations

import os
import shlex
import subprocess


def main() -> None:
    src_dataset = "src/foo/bar"
    dst_dataset = "dst/boo/bar"
    dryrun = os.getenv("DRYRUN", "1") == "1"
    snapshot_plan = {"prod": {"onsite": {"minutely": 40, "hourly": 36, "daily": 31}}}
    prune_plan = {"prod": {"onsite": {"minutely": 40, "hourly": 36, "daily": 31}}}

    cmd: list[str] = ["bzfs", src_dataset, dst_dataset, "--recursive"]

    # Add task flags here, for example:
    # cmd += ["--skip-replication", "--compare-snapshot-lists=src+dst+all"]
    # cmd += [f"--include-snapshot-plan={snapshot_plan}"]
    # cmd += ["--delete-dst-snapshots", f"--delete-dst-snapshots-except-plan={prune_plan}"]

    if dryrun:
        cmd.append("--dryrun")

    print("Review command before run:")
    print("  " + shlex.join(cmd))
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
```

## Bash Template: Periodic bzfs_jobrunner Invocation

```bash
#!/usr/bin/env bash
set -euo pipefail

JOBCONFIG="/bzfs/bzfs_testbed/bzfs_job_testbed.py"
HOSTNAME_VALUE="$(hostname)"
ACTION="${1:---replicate}"  # --create-src-snapshots|--replicate|--prune-dst-snapshots|--monitor-dst-snapshots
DRYRUN="${DRYRUN:-1}"  # keep 1 for safety

cmd=("$JOBCONFIG")

case "$ACTION" in
  --create-src-snapshots|--prune-src-snapshots|--prune-src-bookmarks|--monitor-src-snapshots)
    cmd+=(--src-host="$HOSTNAME_VALUE" "$ACTION")
    ;;
  --replicate|--prune-dst-snapshots|--monitor-dst-snapshots)
    cmd+=(--dst-host="$HOSTNAME_VALUE" "$ACTION")
    ;;
  *)
    echo "Unsupported action: $ACTION" >&2
    exit 2
    ;;
esac

if [[ "$DRYRUN" == "1" ]]; then
  # --dryrun protects bzfs subcommands
  cmd+=(--dryrun)
fi

printf 'Review command before run:\n'
printf '  %q' "${cmd[@]}"
printf '\n'
"${cmd[@]}"
```

## Python Template: Periodic bzfs_jobrunner Invocation

```python
#!/usr/bin/env python3
"""Run one periodic bzfs_jobrunner action with dry-run enabled by default."""

from __future__ import annotations

import os
import shlex
import socket
import subprocess
import sys


def main() -> None:
    jobconfig = "/bzfs/bzfs_testbed/bzfs_job_testbed.py"
    hostname_value = socket.gethostname()
    action = sys.argv[1] if len(sys.argv) > 1 else "--replicate"
    dryrun = os.getenv("DRYRUN", "1") == "1"

    cmd: list[str] = [jobconfig]

    if action in {
        "--create-src-snapshots",
        "--prune-src-snapshots",
        "--prune-src-bookmarks",
        "--monitor-src-snapshots",
    }:
        cmd += [f"--src-host={hostname_value}", action]
    elif action in {"--replicate", "--prune-dst-snapshots", "--monitor-dst-snapshots"}:
        cmd += [f"--dst-host={hostname_value}", action]
    else:
        raise SystemExit(f"Unsupported action: {action}")

    if dryrun:
        # --dryrun protects bzfs subcommands
        cmd += ["--dryrun"]

    print("Review command before run:")
    print("  " + shlex.join(cmd))
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
```

## Dict/List Argument Pattern for bzfs_jobrunner

Use this pattern when the script needs fleet mappings or plans (`--src-hosts`, `--dst-hosts`, `--retain-dst-targets`,
`--dst-root-datasets`, `--src-snapshot-plan`, `--src-bookmark-plan`, `--dst-snapshot-plan`, `--monitor-snapshot-plan`).

### Bash

```bash
SRC_HOSTS="['testsrc01']"
DST_HOSTS="{'testdst01': ['', 'onsite']}"
SRC_SNAPSHOT_PLAN="{'prod': {'onsite': {'hourly': 36, 'daily': 31}}}"

cmd=(
  bzfs_jobrunner
  "--src-hosts=$SRC_HOSTS"
  "--dst-hosts=$DST_HOSTS"
  "--src-snapshot-plan=$SRC_SNAPSHOT_PLAN"
)
```

### Python

```python
src_hosts = ["testsrc01"]
dst_hosts = {"testdst01": ["", "onsite"]}
src_snapshot_plan = {"prod": {"onsite": {"hourly": 36, "daily": 31}}}

cmd = [
    "bzfs_jobrunner",
    f"--src-hosts={src_hosts}",
    f"--dst-hosts={dst_hosts}",
    f"--src-snapshot-plan={src_snapshot_plan}",
]
```

## Minimal Cron Examples

```cron
# Every minute: source snapshot creation + source pruning (still dry-run)
* * * * * /usr/local/bin/bzfs-src-cycle.sh --create-src-snapshots
* * * * * /usr/local/bin/bzfs-src-cycle.sh --prune-src-snapshots

# Every minute: destination replication + destination pruning (still dry-run)
* * * * * /usr/local/bin/bzfs-dst-cycle.sh --replicate
* * * * * /usr/local/bin/bzfs-dst-cycle.sh --prune-dst-snapshots
```
