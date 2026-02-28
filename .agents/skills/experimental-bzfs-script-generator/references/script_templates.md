# Script Templates

Use these templates to generate minimal scripts. Keep dry-run enabled unless a human explicitly changes the toggle.
Default to returning both Bash and Python variants unless the user requests only one language. For `bzfs_jobrunner`
templates, align action and host-routing with `bzfs_testbed/bzfs_job_testbed.py`. For `bzfs_jobrunner` dict/list options,
align with `bzfs_job_testbed.py`: pass values as `--flag={value}`.

## Bash Template: One-Off bzfs Task

```bash
#!/usr/bin/env bash
set -euo pipefail

SRC_DATASET="tank1/src"
DST_DATASET="tank2/bak"
DRYRUN="${DRYRUN:-1}"  # keep 1 for safety

cmd=(bzfs "$SRC_DATASET" "$DST_DATASET" --recursive)

# Add task flags here, for example:
# cmd+=(--skip-replication --compare-snapshot-lists=src+dst+all)
# cmd+=(--create-src-snapshots --create-src-snapshots-plan "{'prod':{'onsite':{'hourly':36}}}")
# cmd+=(--delete-dst-snapshots --delete-dst-snapshots-except-plan "{'prod':{'onsite':{'daily':31}}}")

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
    src_dataset = "tank1/src"
    dst_dataset = "tank2/bak"
    dryrun = os.getenv("DRYRUN", "1") == "1"

    cmd: list[str] = ["bzfs", src_dataset, dst_dataset, "--recursive"]

    # Add task flags here, for example:
    # cmd += ["--skip-replication", "--compare-snapshot-lists=src+dst+all"]

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
ACTION="${1:-replicate}"  # create-src-snapshots|replicate|prune-dst-snapshots|monitor-dst-snapshots
DRYRUN="${DRYRUN:-1}"  # keep 1 for safety

cmd=("$JOBCONFIG")

# Match bzfs_job_testbed.py: source actions run with --src-host; destination actions run with --dst-host.
case "$ACTION" in
  create-src-snapshots|prune-src-snapshots|prune-src-bookmarks|monitor-src-snapshots)
    cmd+=(--src-host="$HOSTNAME_VALUE" "--$ACTION")
    ;;
  replicate|prune-dst-snapshots|monitor-dst-snapshots)
    cmd+=(--dst-host="$HOSTNAME_VALUE" "--$ACTION")
    ;;
  *)
    echo "Unsupported action: $ACTION" >&2
    exit 2
    ;;
esac

if [[ "$DRYRUN" == "1" ]]; then
  # --dryrun protects bzfs subcommands; --jobrunner-dryrun suppresses side effects in jobrunner itself.
  cmd+=(--jobrunner-dryrun --dryrun)
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
    action = sys.argv[1] if len(sys.argv) > 1 else "replicate"
    dryrun = os.getenv("DRYRUN", "1") == "1"

    cmd: list[str] = [jobconfig]

    # Match bzfs_job_testbed.py: source actions use --src-host.
    if action in {
        "create-src-snapshots",
        "prune-src-snapshots",
        "prune-src-bookmarks",
        "monitor-src-snapshots",
    }:
        cmd += [f"--src-host={hostname_value}", f"--{action}"]
    # Match bzfs_job_testbed.py: destination actions use --dst-host.
    elif action in {"replicate", "prune-dst-snapshots", "monitor-dst-snapshots"}:
        cmd += [f"--dst-host={hostname_value}", f"--{action}"]
    else:
        raise SystemExit(f"Unsupported action: {action}")

    if dryrun:
        # --dryrun protects bzfs subcommands; --jobrunner-dryrun suppresses side effects in jobrunner itself.
        cmd += ["--jobrunner-dryrun", "--dryrun"]

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
SRC_HOSTS="['nas']"
DST_HOSTS="{'nas': ['', 'onsite']}"
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
src_hosts = ["nas"]
dst_hosts = {"nas": ["", "onsite"]}
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
* * * * * /usr/local/bin/bzfs-source-cycle.sh create-src-snapshots
* * * * * /usr/local/bin/bzfs-source-cycle.sh prune-src-snapshots

# Every minute: destination replication + destination pruning (still dry-run)
* * * * * /usr/local/bin/bzfs-dst-cycle.sh replicate
* * * * * /usr/local/bin/bzfs-dst-cycle.sh prune-dst-snapshots
```
