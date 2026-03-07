# Task Recipes (Canonical Patterns)

Use these as starting points. Keep dry-run on by default for mutating tasks. These command patterns are
language-agnostic; when turning them into scripts, provide Bash and Python variants unless the user requests one
language.

Read-only tasks (monitoring, compare, inventory) do not require `--dryrun`.

## Conventions

- `SRC`: source dataset (or `user@host:dataset`)
- `DST`: destination dataset (or `user@host:dataset`)
- `dummy`: virtual empty source used for independent retention policies
- `PLAN`: bzfs plan dictionary string

## 1) Create snapshots only (ad hoc/manual)

```bash
bzfs "$SRC" dummy \
  --recursive \
  --skip-replication \
  --create-src-snapshots \
  --create-src-snapshots-plan "$PLAN" \
  --dryrun
```

## 2) Replicate snapshots (backup)

```bash
bzfs "$SRC" "$DST" \
  --recursive \
  --dryrun
```

## 3) Prune destination snapshots by retention plan

```bash
bzfs "$SRC_OR_DUMMY" "$DST" \
  --recursive \
  --skip-replication \
  --delete-dst-snapshots \
  --delete-dst-snapshots-except-plan "$PLAN" \
  --dryrun
```

## 4) Prune bookmarks by retention plan

```bash
bzfs dummy "$DATASET" \
  --recursive \
  --skip-replication \
  --delete-dst-snapshots=bookmarks \
  --delete-dst-snapshots-except-plan "$PLAN" \
  --dryrun
```

## 5) Monitor snapshot age

```bash
bzfs "$SRC" "$DST" \
  --recursive \
  --skip-replication \
  --monitor-snapshots "$MONITOR_PLAN"
```

## 6) Compare source and destination snapshot lists

```bash
bzfs "$SRC" "$DST" \
  --recursive \
  --skip-replication \
  --compare-snapshot-lists=src+dst+all
```

## 7) Restore rehearsal into non-production dataset

```bash
bzfs "$BACKUP_SRC" "$RESTORE_DST" \
  --recursive \
  --dryrun
```

Optional conflict handling flags (keep disabled by default):

- `--force-rollback-to-latest-snapshot`
- `--force-rollback-to-latest-common-snapshot`
- `--force`

## 8) Periodic orchestration with bzfs_jobrunner

Follow `bzfs_testbed/bzfs_job_testbed.py` conventions. When emitting full fleet orchestration commands, pass dict/list
values using `--flag={value}` style that matches `bzfs_job_testbed.py`. Carry over the same semantics as the example
(action-to-host scope, plan intent, and retention meaning), not just syntax.

Source-host periodic tasks:

```bash
/bzfs/bzfs_testbed/bzfs_job_testbed.py \
  --src-host="$(hostname)" \
  --create-src-snapshots \
  --prune-src-snapshots \
  --prune-src-bookmarks \
  --dryrun
```

Destination-host periodic tasks:

```bash
/bzfs/bzfs_testbed/bzfs_job_testbed.py \
  --dst-host="$(hostname)" \
  --replicate \
  --prune-dst-snapshots \
  --dryrun
```

Monitoring:

```bash
/bzfs/bzfs_testbed/bzfs_job_testbed.py \
  --src-host="$(hostname)" \
  --monitor-src-snapshots
```

Fleet mapping and plan arguments (shape only):

```bash
bzfs_jobrunner \
  "--src-hosts=['testsrc01']" \
  "--dst-hosts={'testdst01': ['', 'onsite']}" \
  "--retain-dst-targets={'testdst01': ['', 'onsite']}" \
  "--dst-root-datasets={'testdst01': ''}" \
  "--src-snapshot-plan={'prod': {'onsite': {'hourly': 36, 'daily': 31}}}"
```

## 9) Read-only inventory command (allowed execution during skill use)

```bash
zfs list -t snapshot,bookmark -o name,guid,createtxg,creation -s creation -r "$DATASET"
```
