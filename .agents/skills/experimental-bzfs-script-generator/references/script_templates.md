# Script Templates

Use these templates to generate minimal scripts. Keep dry-run enabled unless a human explicitly changes the toggle.

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

JOBCONFIG="/etc/bzfs/bzfs_job_example.py"
HOSTNAME_VALUE="$(hostname)"
ACTION="${1:-replicate}"  # create-src-snapshots|replicate|prune-dst-snapshots|monitor-dst-snapshots
DRYRUN="${DRYRUN:-1}"  # keep 1 for safety

cmd=("$JOBCONFIG")

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
  cmd+=(--jobrunner-dryrun --dryrun)
fi

printf 'Review command before run:\n'
printf '  %q' "${cmd[@]}"
printf '\n'
"${cmd[@]}"
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
