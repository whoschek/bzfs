---
name: experimental-bzfs-script-generator
description: 'Generate or change idiomatic minimal Bash and Python scripts that use bzfs and bzfs_jobrunner for ZFS snapshot management workflows in a sandboxed test VM: snapshot creation, replication/backup, restore rehearsal, snapshot pruning, snapshot monitoring, and snapshot list comparison. Use when asked to create or change ad hoc/manual or periodic/automatic scripts for these tasks. Do not use this skill for general ZFS administration or non-bzfs tooling.'
---

# Experimental bzfs Script Generator

## Core Outcome

Generate reviewable snapshot management scripts, not command transcripts. Keep scripts minimal, idiomatic, and explicit
about safety knobs and assumptions. Enforce safety-first dry-run defaults for mutating flows (`bzfs --dryrun`;
`bzfs_jobrunner --dryrun` (without `--jobrunner-dryrun`) and do not execute non-read-only CLIs. For bzfs_jobrunner
outputs, fit all idiomatic patterns from bzfs_testbed/bzfs_job_testbed.py (both syntactic and semantic), including
action routing and dict construction/format/passing.

## Hard Safety Rules

01. No TDD by default: Do not create unit tests or integregation tests unless the User explicitly requests it.
02. Generate scripts only. Never execute generated scripts.
03. Never execute CLI commands while using this skill, except optional read-only `zfs list ...` and `zpool list ...`
    commands for pool/dataset/snapshot discovery.
04. Classify create/replicate/prune/rollback/restore flows as state-changing.
05. For state-changing flows, default scripts to dry-run:
    - `bzfs ... --dryrun` (default `send`; use `--dryrun=recv` only if asked).
    - `bzfs_jobrunner ... --dryrun`.
06. Use `DRYRUN` (Bash) / `dryrun` (Python) as the dry-run toggle variable. Keep safe defaults enabled (`DRYRUN=1`,
    `dryrun=True`).
07. Keep destructive flags (`--force*`, `--delete-*`) opt-in, documented, and disabled by default.
08. Prefer `bzfs` and `bzfs_jobrunner` over direct mutating `zfs` commands.
09. Keep scope limited to bzfs and bzfs_jobrunner snapshot management workflows; decline general ZFS administration or
    non-bzfs tooling requests.
10. Do not favor Bash over Python; provide both Bash and Python script outputs unless the user asks for one language.
11. For `bzfs_jobrunner` scripts, mirror the action set from `bzfs_testbed/bzfs_job_testbed.py`.
12. For `bzfs_jobrunner` scripts, mirror dict handling from `bzfs_testbed/bzfs_job_testbed.py`: build native dict/list
    objects in Python and pass them via `--flag={value}` formatting; in Bash, keep explicit quoted dict literals.
13. Distill and apply `bzfs_job_testbed.py` idioms at both syntactic and semantic levels.

## Step by Step Reasoning Workflow:

- Think systematically through what's been asked of you, break down the problem, work through it step by step, and
  reason deeply before responding.

## Script Generation Workflow

1. Gather required inputs:

   - datasets, recursion scope, hostnames and their roles, replication schedule, retention plans, monitoring plans, log
     path.
   - If inputs are missing, ask the User corresponding questions via the `request_user_input` tool or similar, if
     available.

2. Classify the request:

   - `read_only`: snapshot list compare, snapshot monitoring, inventory/listing.
   - `state_changing`: snapshot creation, replication/backup, snapshot pruning, restore.

3. Choose the CLI:

- Prefer direct `bzfs` for:

  - adhoc/manual snapshot creation,
  - adhoc/manual replication or restore,
  - adhoc/manual snapshot pruning,
  - adhoc/manual monitoring of snapshots,
  - snapshot list comparison.

- Prefer `bzfs_jobrunner` for:

  - periodic or automatic workflows,
  - multi-host or fleet-wide orchestration,
  - one shared jobconfig that drives create snapshot, replicate, prune, and monitor actions,
  - cron/systemd style wrappers around a shared Python config.

4. `bzfs_jobrunner` Host filtering:

   - This is a complex area. Think deeply. Source-side actions usually scope with `--src-host`, destination-side actions
     with `--dst-host`. But do not follow this template blindly; depending on which specific source/destination host
     subsets the workflow is actually intended for (for example third-party-host orchestration, testing one src -> dst
     route, each destination host pulling independently or each source host pushing independently, or high-frequency
     pair jobs), add or omit `--src-host` and/or `--dst-host` filters.
   - You have plenty of time; go slow and make sure everything is correct.

5. Generate code:

   - For `bzfs_jobrunner` NEVER merge any job actions (for example `--create-src-snapshots`, `--replicate`,
     `--prune-src-snapshots`, `--prune-src-bookmarks`, `--prune-dst-snapshots`, `--monitor-src-snapshots`,
     `--monitor-dst-snapshots`, `--dryrun`, `--verbose`) into the underlying jobconfig script code; instead keep them in
     a separate launcher bash script so different actions can run with the same jobconfig configuration settings.
   - Prefer plan-based convenience flags such as `--include-snapshot-plan`, `--create-src-snapshots-plan`, and
     `--delete-dst-snapshots-except-plan` over hand-written `--include-snapshot-times-and-ranks` chains when standard
     secondly/minutely/hourly/daily/weekly/monthly/yearly policies are sufficient.
   - Keep code idiomatic and minimal. Rigorously apply the KISS principle: Keep it simple stupid. Leave out all fluff
     and unnecessary indirections/abstractions. Do not add any CLI flag. Do not add any environment variable beyond
     DRYRUN.
   - Bash: `#!/usr/bin/env bash` + `set -euo pipefail`.
   - Python: `#!/usr/bin/env python3` + `subprocess.run([...], check=True)`.
   - Build commands as arrays/lists, not unsafe concatenated shell strings.
   - Model command shape after `bzfs_testbed/bzfs_job_testbed.py` action conventions.
   - For `bzfs_jobrunner` dict args, follow `bzfs_job_testbed.py` style: Python dict/list objects rendered into
     `--src-hosts=...`, `--dst-hosts=...`, `--src-snapshot-plan=...`, etc.
   - Return paired Bash and Python variants by default.

6. Add operator gates:

   - A single dry-run switch defaulting to enabled.
   - Printed command preview before execution.
   - For Python previews, use `shlex.join(cmd)`.
   - For `bzfs_jobrunner` scripts, include `--dryrun` when `DRYRUN=1`; do not include `--jobrunner-dryrun` unless
     explicitly requested otherwise.

7. Return three artifacts:

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
