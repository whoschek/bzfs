# Safety and Semantics for bzfs Script Generation

## Non-Negotiable Defaults

- Generate scripts only. Do not execute generated scripts.
- Keep scope on bzfs and bzfs_jobrunner snapshot management workflows only.
- Decline general ZFS administration and non-bzfs tooling requests.
- Do not favor Bash over Python; default to paired language outputs.
- For `bzfs_jobrunner` scripts, follow `bzfs_tests/bzfs_job_example.py` action and host-routing idioms.
- For `bzfs_jobrunner` dict/list CLI args, follow `bzfs_job_example.py` style: build native Python objects, then pass
  them as `--flag={value}` strings.
- Mirror `bzfs_job_example.py` at both syntactic and semantic levels.
- While using this skill, do not run commands except optional read-only `zfs list` queries.
- For state-changing workflows, keep dry-run enabled by default:
  - `bzfs ... --dryrun`
  - `bzfs_jobrunner ... --dryrun` (usually with `--jobrunner-dryrun`)
- Keep real-run transitions explicit and operator-controlled.

## bzfs Safety-Critical Semantics

- `bzfs` treats source as read-only unless explicitly asked to create snapshots.
- `--dryrun` treats source and destination as read-only.
- Pruning/cleanup options (`--delete-*`) are dangerous and must remain dry-run by default.
- `--force-rollback-to-latest-snapshot` is less invasive than `--force-rollback-to-latest-common-snapshot`, which is
  less invasive than `--force`, which is less invasive than `--force-destroy-dependents`.
- For pruning via `--delete-dst-snapshots`, cross-check behavior matters: using a real source differs from using `dummy`
  source retention policy.
- Replication safety improves with bookmarks (`--create-bookmarks` default is safety-oriented; `--no-use-bookmark` is
  discouraged).

## ZFS Semantics Used by bzfs

- `zfs send -i` sends one incremental step. `zfs send -I` sends intermediates.
- Incremental receive requires destination filesystem to exist and its most recent snapshot to match the incremental
  source.
- `zfs receive -s` keeps resumable state after interruption.
- Resume uses `zfs send -t <receive_resume_token>`.
- A bookmark can be incremental source and carries the source snapshot identity metadata required for incremental
  continuity.
- `guid` remains stable across send/receive and is suitable for cross-pool snapshot identity checks.
- `createtxg` is suitable for strict snapshot ordering.
- `snapshots_changed` provides a fast signal that snapshot lists changed.
- `zfs destroy` and `zfs rollback` are destructive; dry-run flags exist in raw ZFS, but this skill should still prefer
  `bzfs` wrappers.

## Pre-Output Checklist

- Script stays minimal and uses idiomatic Bash/Python.
- Output includes Bash and Python variants unless user requested otherwise.
- Dry-run is enabled by default for any mutating workflow.
- Risky flags are commented and disabled unless explicitly requested.
- Placeholder values are explicit and listed after the script.
- No direct mutating `zfs` command is emitted when `bzfs` can do the job.
