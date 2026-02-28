---
name: experimental-bzfs-script-generator
description: 'Generate or change idiomatic minimal Bash and Python scripts that use bzfs and bzfs_jobrunner for ZFS snapshot management workflows in a sandboxed test VM: snapshot creation, replication/backup, restore rehearsal, snapshot pruning, snapshot monitoring, and snapshot list comparison. Use when asked to create or change ad hoc/manual or periodic/automatic scripts for these tasks. Do not use this skill for general ZFS administration or non-bzfs tooling.'
---

# Experimental bzfs Script Generator

## Core Outcome

Generate reviewable snapshot management scripts, not command transcripts. Keep scripts minimal, idiomatic, and explicit
about safety knobs and assumptions. Enforce safety-first dry-run defaults for mutating flows (`bzfs --dryrun`;
`bzfs_jobrunner --dryrun`, typically with `--jobrunner-dryrun`) and do not execute non-read-only CLIs. For
bzfs_jobrunner outputs, fit all idiomatic patterns from bzfs_testbed/bzfs_job_testbed.py (both syntactic and semantic),
including action/host routing and dict construction/format/passing.

## Hard Safety Rules

01. Generate scripts only. Never execute generated scripts.
02. Never execute CLI commands while using this skill, except optional read-only `zfs list ...` commands for
    dataset/snapshot discovery.
03. Classify create/replicate/prune/rollback/restore flows as state-changing.
04. For state-changing flows, default scripts to dry-run:
    - `bzfs ... --dryrun` (default `send`; use `--dryrun=recv` only if asked).
    - `bzfs_jobrunner ... --jobrunner-dryrun --dryrun`.
05. Use `DRYRUN` (Bash) / `dryrun` (Python) as the dry-run toggle variable. Keep safe defaults enabled (`DRYRUN=1`,
    `dryrun=True`).
06. Keep destructive flags (`--force*`, `--delete-*`) opt-in, documented, and disabled by default.
07. Prefer `bzfs` and `bzfs_jobrunner` over direct mutating `zfs` commands.
08. Keep scope limited to bzfs and bzfs_jobrunner snapshot management workflows; decline general ZFS administration or
    non-bzfs tooling requests.
09. Do not favor Bash over Python; provide both Bash and Python script outputs unless the user asks for one language.
10. For `bzfs_jobrunner` scripts, mirror the action set and host routing style from `bzfs_testbed/bzfs_job_testbed.py`
    (`--src-host` for source actions, `--dst-host` for destination actions).
11. For `bzfs_jobrunner` scripts, mirror dict handling from `bzfs_testbed/bzfs_job_testbed.py`: build native dict/list
    objects in Python and pass them via `--flag={value}` formatting; in Bash, keep explicit quoted dict literals.
12. Distill and apply `bzfs_job_testbed.py` idioms at both syntactic and semantic levels.

## Script Generation Workflow

1. Classify the request:
   - `read_only`: snapshot list compare, snapshot monitoring, inventory/listing.
   - `state_changing`: snapshot creation, replication/backup, snapshot pruning, restore.
2. Choose the CLI:
   - Use `bzfs` for ad hoc/manual workflows.
   - Use `bzfs_jobrunner` for periodic or fleet-wide orchestration.
   - For `bzfs_jobrunner`, model command shape after `bzfs_testbed/bzfs_job_testbed.py` action conventions.
   - If the request is outside bzfs/bzfs_jobrunner snapshot management, state that it is out of scope and ask for a
     bzfs-focused target.
3. Gather required inputs:
   - datasets, recursion scope, host/mode, retention plans, schedule, log path.
   - If missing, ask focused questions or emit explicit placeholders (`SRC_DATASET`, `DST_DATASET`) and call them out.
4. Generate idiomatic minimal code:
   - Bash: `#!/usr/bin/env bash` + `set -euo pipefail`.
   - Python: `#!/usr/bin/env python3` + `subprocess.run([...], check=True)`.
   - Build commands as arrays/lists, not unsafe concatenated shell strings.
   - For `bzfs_jobrunner` dict args, follow `bzfs_job_testbed.py` style: Python dict/list objects rendered into
     `--src-hosts=...`, `--dst-hosts=...`, `--src-snapshot-plan=...`, etc.
   - Return paired Bash and Python variants by default.
5. Add operator gates:
   - A single dry-run switch defaulting to enabled.
   - Printed command preview before execution.
   - For Python previews, use `shlex.join(cmd)`.
   - For `bzfs_jobrunner` scripts, include `--dryrun` when `DRYRUN=1`; include `--jobrunner-dryrun` unless explicitly
     requested otherwise.
6. Return three artifacts:
   - scripts (Bash + Python, unless one language is explicitly requested),
   - what it does,
   - exactly what must be edited before first run.

## Output Contract

- Prefer single-purpose scripts.
- Provide Bash and Python variants together unless the user requests otherwise.
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
- Match `bzfs_jobrunner` structure to [bzfs_testbed/bzfs_job_testbed.py](../../../bzfs_testbed/bzfs_job_testbed.py) when
  generating periodic orchestration scripts.
