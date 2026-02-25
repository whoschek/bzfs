---
name: experimental-bzfs-script-generator
description: 'Generate idiomatic, minimal Bash or Python scripts that use bzfs and bzfs_jobrunner for ZFS workflows in a sandboxed test VM: snapshot creation, replication/backup, restore rehearsal, pruning, monitoring, and snapshot-list comparison. Use when asked to create or adapt ad hoc/manual or periodic/automatic scripts for these tasks. Enforce safety-first dry-run defaults for mutating flows (`bzfs --dryrun`; `bzfs_jobrunner --jobrunner-dryrun --dryrun`) and do not execute non-read-only CLIs.'
---

# Experimental Bzfs Script Generator

## Core Outcome

Generate a reviewable script, not a command transcript. Keep scripts minimal, idiomatic, and explicit about safety knobs
and assumptions.

## Hard Safety Rules

1. Generate scripts only. Never execute generated scripts.
2. Never execute CLI commands while using this skill, except optional read-only `zfs list ...` commands for
   dataset/snapshot discovery.
3. Classify create/replicate/prune/rollback/restore flows as state-changing.
4. For state-changing flows, default scripts to dry-run:
   - `bzfs ... --dryrun` (default `send`; use `--dryrun=recv` only if asked).
   - `bzfs_jobrunner ... --jobrunner-dryrun --dryrun`.
5. Use `DRYRUN` (Bash) / `dryrun` (Python) as the dry-run toggle variable. Keep safe defaults enabled (`DRYRUN=1`,
   `dryrun=True`).
6. Keep destructive flags (`--force*`, `--delete-*`) opt-in, documented, and disabled by default.
7. Prefer `bzfs` and `bzfs_jobrunner` over direct mutating `zfs` commands.

## Script Generation Workflow

1. Classify the request:
   - `read_only`: compare, monitor, inventory/listing.
   - `state_changing`: snapshotting, backup/replication, pruning, restore.
2. Choose the CLI:
   - Use `bzfs` for ad hoc/manual workflows and simple periodic scripts.
   - Use `bzfs_jobrunner` for host-mapped/fleet periodic orchestration.
3. Gather required inputs:
   - datasets, recursion scope, host/mode, retention plans, schedule, log path.
   - If missing, ask focused questions or emit explicit placeholders (`SRC_DATASET`, `DST_DATASET`) and call them out.
4. Generate idiomatic minimal code:
   - Bash: `#!/usr/bin/env bash` + `set -euo pipefail`.
   - Python: `#!/usr/bin/env python3` + `subprocess.run([...], check=True)`.
   - Build commands as arrays/lists, not unsafe concatenated shell strings.
5. Add operator gates:
   - A single dry-run switch defaulting to enabled.
   - Printed command preview before execution.
   - For Python previews, use `shlex.join(cmd)`.
6. Return three artifacts:
   - script,
   - what it does,
   - exactly what must be edited before first run.

## Output Contract

- Prefer single-purpose scripts.
- Keep comments short and safety-focused.
- Keep naming aligned with bzfs docs (`org`, `target`, `*_plan`).
- Include cron/systemd-ready command lines for periodic tasks.
- For "restore", default to restore rehearsal into non-production target datasets unless explicitly instructed
  otherwise.

## References

- Read [references/safety_and_semantics.md](references/safety_and_semantics.md) before generating state-changing
  workflows.
- Use [references/task_recipes.md](references/task_recipes.md) for canonical task-to-command patterns.
- Start from [references/script_templates.md](references/script_templates.md) for minimal Bash/Python script skeletons.
- Check [references/simulation_examples.md](references/simulation_examples.md) for calibrated examples and quality bar.
