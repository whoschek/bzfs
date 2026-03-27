- Analyze the bzfs repo and create an extremely high quality skill in .agents/skills/bzfs-experimental-script-generator
  for generating (or changing) idiomatic simple minimal bash or Python scripts that use the `bzfs` and `bzfs_jobrunner`
  CLIs to safely perform ZFS snapshot management tasks for snapshot creation, replication/backup, restore, snapshot
  pruning, snapshot monitoring, snapshot list comparison, both adhoc/manual and periodic/automatic. The generated
  scripts are intended for use inside of a sandboxed test VM.

- Do not use the skill for general ZFS administration or non-bzfs tooling.

- Choose the CLI:
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

- `bzfs_jobrunner` Host filtering:
  - This is a complex area. Think deeply. Source-side actions usually scope with `--src-host`, destination-side actions
    with `--dst-host`. But do not follow this template blindly; depending on which specific source/destination host
    subsets the workflow is actually intended for (for example third-party-host orchestration, testing one src -> dst
    route, each destination host pulling independently or each source host pushing independently, or high-frequency pair
    jobs), add or omit `--src-host` and/or `--dst-host` filters.
  - You have plenty of time; go slow and make sure everything is correct.

- For `bzfs_jobrunner` NEVER merge any job actions (for example `--create-src-snapshots`, `--replicate`,
  `--prune-src-snapshots`, `--prune-src-bookmarks`, `--prune-dst-snapshots`, `--monitor-src-snapshots`,
  `--monitor-dst-snapshots`, `--dryrun`, `--verbose`) into the underlying jobconfig script code; instead keep them in a
  separate launcher bash script so different actions can run with the same jobconfig configuration settings.

- For `bzfs_jobrunner` scripts, analyze and distill all idiomatic patterns in `bzfs_testbed/bzfs_job_testbed.py` (both
  syntactic and semantic), including how to construct, format and pass dicts, and follow the same idiomatic patterns in
  generated scripts.

- Prefer plan-based convenience flags such as `--include-snapshot-plan`, `--create-src-snapshots-plan`, and
  `--delete-dst-snapshots-except-plan` over hand-written `--include-snapshot-times-and-ranks` chains when standard
  secondly/minutely/hourly/daily/weekly/monthly/yearly policies are sufficient.

- Keep code idiomatic and minimal. Rigorously apply the KISS principle: Keep it simple stupid. Leave out all fluff and
  unnecessary indirections/abstractions. Do not add any CLI flag. Do not add any environment variable beyond DRYRUN.

- Do not favor bash over Python - always create scripts, examples, templates, proposals, etc in both programming
  languages unless the User requests otherwise.

- Safety is top priority. All generated scripts that change or delete ZFS state MUST default to `bzfs --dryrun ...` and
  `bzfs_jobrunner --dryrun ...` (without `--jobrunner-dryrun`) via a `DRYRUN` Unix environment variable. The resulting
  skill MUST NEVER execute CLI commands except for read-only `zfs list` and read-only `zpool list` - the skill shall
  ONLY generate scripts that Users carefully review and maybe later test according to their own judgement.

- Spend _A LOT OF TIME_ to think this through very deeply, spend at least 30 minutes of work on this, because the issues
  are subtle and complex and safety critical and any change here has high impact.

- If you have questions regarding the semantics of the underlying `zfs` CLI, consult the ../zfs/man directory.

- Write this prompt to assets/skill_creation_prompt.md inside of the skill directory.
