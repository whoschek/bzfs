Analyze the bzfs repo and create an extremely high quality skill in .agents/skills/experimental-bzfs-script-generator
for generating idiomatic simple minimal bash or python scripts that use the `bzfs` and `bzfs_jobrunner` CLIs to safely
perform typical ZFS tasks, including snapshotting, backup, restore, pruning, monitoring, comparing snapshot lists, etc,
both adhoc/manual and periodic/automatic. The generated scripts are intended for use inside of a sandboxed test VM.

Safety is top priority. All generated scripts that change or delete ZFS state MUST default to `bzfs --dryrun ...` and
`bzfs_jobrunner --dryrun ...` via a `DRYRUN` Unix environment variable. The resulting skill MUST NEVER execute CLI
commands except for read-only `zfs list` - the skill shall ONLY generate scripts that Users carefully review and maybe
later test according to their own judgement.

Spend *A LOT OF TIME* to think this through very deeply, spend at least 30 minutes of work on this, because the issues
are subtle and complex and safety critical and any change here has high impact.

If you have questions regarding the semantics of the underlying `zfs` CLI, consult the ../zfs/man directory. Write this
prompt to assets/skill_creation_prompt.md inside of the skill folder.
