# Getting Started with bzfs_joblauncher

This friendly guide shows how to set up `bzfs_joblauncher` so **hostA** regularly snapshots, replicates, and prunes the
datasets `tank1/foo` and `tank1/bar` to **hostB**. Both hosts run OpenZFS and `bzfs` is installed on hostA.

## 1. Install bzfs with joblauncher support

```
pip install 'bzfs[bzfs_joblauncher]'
```

## 2. Create `/etc/bzfs/bzfs_job_example.yaml`

Save the following YAML as `/etc/bzfs/bzfs_job_example.yaml` on hostA. It captures the `prod/offsite` policy {
"5minutely": 10, "hourly": 72, "daily": 90 } and runs every five minutes. List each source dataset followed by its
destination; when the names are the same, repeat the dataset within the pair.

```
root_dataset_pairs:
  - [tank1/foo, tank1/foo]  # replicate to same name on hostB
  - [tank1/bar, tank1/bar]  # replicate to same name on hostB
recursive: true  # include child datasets

src_hosts:
  - hostA  # source taking snapshots

dst_hosts:
  hostB:
    - offsite  # logical target name embedded in snapshot names

retain_dst_targets:
  hostB:
    - offsite  # keep offsite snapshots on hostB

dst_root_datasets:
  hostB: ''  # keep original dataset names on destination

# Retention periods for snapshots to be used if pruning src, and when creating new snapshots on src.
# For example, "daily": 31 specifies to retain all daily snapshots that were created less than 31 days ago, and
# ensure that the latest 31 daily snapshots (per dataset) are retained regardless of creation time.
# A zero or missing retention period indicates that no snapshots shall be retained (or even be created) for the given period.
# Each target of each organization can have separate retention periods.
# Uses snapshot names like 'prod_onsite_<timestamp>_daily', 'prod_onsite_<timestamp>_minutely', etc.:
src_snapshot_plan:
  prod:
    offsite:
      5minutely: 10
      hourly: 72
      daily: 90

# Retention periods for snapshots to be used if pruning dst. Has same format as --src-snapshot-plan:
dst_snapshot_plan:
  prod:
    offsite:
      5minutely: 10
      hourly: 72
      daily: 90

# Retention periods for bookmarks to be used if pruning src. Has same format as --src-snapshot-plan:
src_bookmark_plan:
  prod:
    offsite:
      5minutely: 10
      hourly: 72
      daily: 90

monitor_snapshot_plan:
  prod:
    offsite:
      5minutely:
        warning: 15 minutes
        critical: 30 minutes
      hourly:
        warning: 3 hours
        critical: 6 hours
      daily:
        warning: 2 days
        critical: 4 days

# Max worker jobs to run in parallel at any time; specified as a positive integer, or as a percentage of num CPU cores:
workers: "100%"        # aka max_workers = 1.0 * num_cores

```

## 3. Run the job once

Start with a dry run to safely preview what would happen:

```
bzfs_joblauncher /etc/bzfs/bzfs_job_example.yaml --jobrunner-dryrun \
  --create-src-snapshots --replicate \
  --prune-src-snapshots --prune-src-bookmarks \
  --prune-dst-snapshots --monitor-src-snapshots \
  --monitor-dst-snapshots
```

`--jobrunner-dryrun` prints the planned operations without changing any ZFS state.

If the output looks correct, remove `--jobrunner-dryrun` so the job runs but add `--dryrun` to keep `bzfs` from changing
ZFS state:

```
bzfs_joblauncher /etc/bzfs/bzfs_job_example.yaml --dryrun \
  --create-src-snapshots --replicate \
  --prune-src-snapshots --prune-src-bookmarks \
  --prune-dst-snapshots --monitor-src-snapshots \
  --monitor-dst-snapshots
```

When you are satisfied with the simulated output, drop `--dryrun` to perform the real operations:

```
bzfs_joblauncher /etc/bzfs/bzfs_job_example.yaml \
  --create-src-snapshots --replicate \
  --prune-src-snapshots --prune-src-bookmarks \
  --prune-dst-snapshots --monitor-src-snapshots \
  --monitor-dst-snapshots
```

A `0` exit status means snapshots were taken, replicated, and pruned successfully.

## 4. Schedule periodic runs

Add a cron entry on hostA to repeat the job every five minutes:

```
*/5 * * * * bzfs_joblauncher /etc/bzfs/bzfs_job_example.yaml --create-src-snapshots --replicate \
  --prune-src-snapshots --prune-src-bookmarks --prune-dst-snapshots \
  --monitor-src-snapshots --monitor-dst-snapshots >>/var/log/bzfs_job.log 2>&1
```

## 5. Monitor health

- **Snapshot schedule:** `monitor_snapshot_plan` above defines warning and critical thresholds. With
  `--monitor-src-snapshots` and `--monitor-dst-snapshots`, the launcher exits non‑zero and logs a message if snapshots
  fall behind on either host.
- **Replication:** The same monitoring flags also confirm snapshots are arriving on hostB on time.
- **Pruning:** The `--prune-*` flags remove outdated snapshots and bookmarks. Check `/var/log/bzfs_job.log` or run
  `zfs list -t snapshot tank1/foo@* tank1/bar@*` on both hosts to confirm only 10 five‑minute, 72 hourly and 90 daily
  snapshots remain.

To run a quick health check at any time, add `-v` for verbose detail about which snapshots are late:

```
bzfs_joblauncher /etc/bzfs/bzfs_job_example.yaml -v --monitor-src-snapshots --monitor-dst-snapshots
```

Here `-v` increases log verbosity. A `0` exit code means the system is healthy; non‑zero indicates what fell behind.

## 6. Enjoy

You now have an automated replication setup that snapshots, replicates, prunes and monitors itself. Extend it by adding
more dataset pairs, hosts, or `bzfs` options as your environment grows.
