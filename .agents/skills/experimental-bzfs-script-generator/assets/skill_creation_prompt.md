- Analyze the bzfs repo and create an extremely high quality skill in .agents/skills/experimental-bzfs-script-generator
  for generating (or changing) idiomatic simple minimal bash or Python scripts that use the `bzfs` and `bzfs_jobrunner`
  CLIs to safely perform ZFS snapshot management tasks for snapshot creation, replication/backup, restore, snapshot
  pruning, snapshot monitoring, snapshot list comparison, both adhoc/manual and periodic/automatic. The generated
  scripts are intended for use inside of a sandboxed test VM.

- Do not use the skill for general ZFS administration or non-bzfs tooling.

- Do not favor bash over Python - always create scripts, examples, templates, proposals, etc in both programming
  languages unless the User requests otherwise.

- For `bzfs_jobrunner` scripts, analyze and distill all idiomatic patterns in `bzfs_testbed/bzfs_job_testbed.py` (both
  syntactic and semantic), including how to construct, format and pass dicts, and follow the same idiomatic patterns in
  generated scripts.

- Safety is top priority. All generated scripts that change or delete ZFS state MUST default to `bzfs --dryrun ...` and
  `bzfs_jobrunner --dryrun ...` via a `DRYRUN` Unix environment variable. The resulting skill MUST NEVER execute CLI
  commands except for read-only `zfs list` - the skill shall ONLY generate scripts that Users carefully review and maybe
  later test according to their own judgement.

- Spend *A LOT OF TIME* to think this through very deeply, spend at least 30 minutes of work on this, because the issues
  are subtle and complex and safety critical and any change here has high impact.

- If you have questions regarding the semantics of the underlying `zfs` CLI, consult the ../zfs/man directory.

- Write this prompt to assets/skill_creation_prompt.md inside of the skill directory.
