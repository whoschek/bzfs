<!--
 Copyright 2024 Wolfgang Hoschek AT mac DOT com

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.-->

# Tiered Snapshot Backup: srchost --> bckphost --> archost

A ready-to-adapt deployment of [bzfs_jobrunner](../README_bzfs_jobrunner.md) for a three-tier chain in which each host
has a different availability profile:

| Host       | Availability                               | Role                                                                       |
| ---------- | ------------------------------------------ | -------------------------------------------------------------------------- |
| `srchost`  | powered on only while somebody works on it | takes 15-minutely, hourly and daily snapshots and **pushes** them onward   |
| `bckphost` | always on, low power, modest storage       | keeps a sliding one-week window; also runs all the always-on health checks |
| `archost`  | online at an indeterminate time each day   | **pulls** whatever accumulated and keeps daily snapshots forever           |

Snapshots are created exactly once, on `srchost`, and the very same snapshots then travel down the chain, because
`zfs send` preserves snapshot names. Only the retention differs per tier. A snapshot is named
`backup_onsite_<timestamp>_15minutely` (organization `backup`, target `onsite`, then the period).

## Files

| File                         | Purpose                                                                                |
| ---------------------------- | -------------------------------------------------------------------------------------- |
| `bzfs_job_src_to_bckp.py`    | tier-1 jobconfig: hosts, datasets and retention for `srchost` --> `bckphost`           |
| `bzfs_job_bckp_to_arc.py`    | tier-2 jobconfig: hosts, datasets and retention for `bckphost` --> `archost`           |
| `bzfs_backup_run.sh` / `.py` | single entry point; maps a job name to jobconfig + host filter + actions               |
| `bzfs_alert.sh` / `.py`      | watchdog: runs one job, alerts only after a grace period, and alerts again only rarely |
| `bzfs_notify.sh`             | notification transport; edit this to reach email / ntfy / Gotify / Pushover            |

The `.sh` and `.py` variants of `bzfs_backup_run` and `bzfs_alert` are equivalent and share the same state file format.
**Deploy one language pair and delete the other**, so that a given job name means exactly one thing on a given host.

All five files are deployed unchanged to all three hosts; the job name decides what actually runs where.

## Retention

| Period       | `srchost`    | `bckphost`   | `archost`        |
| ------------ | ------------ | ------------ | ---------------- |
| `15minutely` | 192 (2 days) | 672 (7 days) | 2880 (30 days)   |
| `hourly`     | 96 (4 days)  | 168 (7 days) | 720 (30 days)    |
| `daily`      | 30           | 30           | 36500 (~forever) |

Two of these numbers are less obvious than they look:

- **`srchost` keeps far more than one push interval.** If pushing breaks, `srchost` keeps snapshotting. Anything it
  prunes before `bckphost` received it is lost for good, so the source window has to be wider than the outage you expect
  to notice and repair.
- **`bckphost` keeps dailies for 30 days, not 7.** `archost` is offline most of the time, and a daily snapshot that
  `bckphost` prunes before `archost` fetched it leaves a permanent hole in the archive. 30 days of dailies is the budget
  for how long `archost` may stay away. The one-week window in the request applies to the sub-daily snapshots, which is
  where the bulk of the storage sits anyway. Raise `daily` in tier 1's `dst_snapshot_plan` if `archost` can be gone
  longer.

Bookmarks do the rest of the work here. `bzfs` leaves a bookmark behind on the sending side for every snapshot it sends,
and a bookmark still works as an incremental source after the snapshot itself is gone. Tier 2 therefore keeps daily
bookmarks on `bckphost` forever: however long `archost` stays offline, it can always resume incrementally instead of
re-sending the whole dataset. Bookmarks cost no space.

## What you must edit before the first run

In `bzfs_job_src_to_bckp.py`:

1. `src_host`, `bckp_host` -- the real output of the `hostname` CLI on those machines.
2. `root_dataset_pairs` -- the datasets to protect, e.g. `["tank/work", f"{src_host}/work"]`.
3. `dst_root_datasets` -- the pool or dataset on `bckphost` that holds backups, e.g. `{bckp_host: "bak"}`.

In `bzfs_job_bckp_to_arc.py`, the following must match tier 1 exactly, because tier 2's source is tier 1's destination:

| Tier 2 setting       | Must equal                                    |
| -------------------- | --------------------------------------------- |
| `src_host`           | tier 1 `src_host`                             |
| `bckp_host`          | tier 1 `bckp_host`                            |
| `org`, `target`      | tier 1 `org`, `target`                        |
| `bckp_root_dataset`  | tier 1 `dst_root_datasets[bckp_host]`         |
| `root_dataset_pairs` | the dst side of tier 1's `root_dataset_pairs` |
| `src_snapshot_plan`  | tier 1 `dst_snapshot_plan`                    |

Then set `arc_host` and `dst_root_datasets` for the archive pool.

Finally, edit `bzfs_notify.sh` to actually reach a human, and adjust the grace periods in `bzfs_alert.sh` /
`bzfs_alert.py` if the defaults do not fit your habits.

## Jobs and schedule

`bzfs_backup_run.sh <job>` is the only thing you schedule. Every job runs under the watchdog.

| Job              | Runs on    | Suggested cadence | What it does                                              |
| ---------------- | ---------- | ----------------- | --------------------------------------------------------- |
| `src-to-bckp`    | `srchost`  | every 15 min      | create snapshots, push to `bckphost`, prune `srchost`     |
| `bckp-prune`     | `bckphost` | hourly            | enforce the sliding window on `bckphost`                  |
| `bckp-freshness` | `bckphost` | hourly            | alert if `srchost` stopped delivering altogether          |
| `bckp-bookmarks` | `bckphost` | daily             | bound bookmark growth on `bckphost`                       |
| `arc-freshness`  | `bckphost` | hourly            | alert if `archost` stopped fetching or became unreachable |
| `bckp-to-arc`    | `archost`  | every 15 min      | pull from `bckphost`, prune the archive                   |

Within `src-to-bckp` and `bckp-to-arc`, `bzfs_jobrunner` runs the actions in the listed order, so replication always
happens before pruning -- never the other way round.

### systemd (recommended, especially on `srchost`)

`Persistent=true` makes the timer fire once soon after the machine wakes up if the scheduled time passed while it was
asleep, which is exactly the "snapshot whenever `srchost` is active" behaviour. `/opt/bzfs-backup` below is wherever you
deployed these files.

```ini
# /etc/systemd/system/bzfs-backup@.service
[Unit]
Description=bzfs tiered backup job %i
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=backup
Environment=DRYRUN=1
Environment=PATH=/usr/local/bin:/usr/bin:/bin
ExecStart=/opt/bzfs-backup/bzfs_backup_run.sh %i
```

```ini
# /etc/systemd/system/bzfs-backup@src-to-bckp.timer   (on srchost)
[Unit]
Description=bzfs tiered backup job src-to-bckp

[Timer]
OnCalendar=*:0/15
Persistent=true
RandomizedDelaySec=60

[Install]
WantedBy=timers.target
```

```
systemctl enable --now bzfs-backup@src-to-bckp.timer                                  # on srchost
systemctl enable --now bzfs-backup@{bckp-prune,bckp-freshness,arc-freshness}.timer    # on bckphost, OnCalendar=hourly
systemctl enable --now bzfs-backup@bckp-bookmarks.timer                               # on bckphost, OnCalendar=daily
systemctl enable --now bzfs-backup@bckp-to-arc.timer                                  # on archost,  OnCalendar=*:0/15
```

### cron (alternative)

```cron
# srchost
*/15 * * * * backup DRYRUN=1 /opt/bzfs-backup/bzfs_backup_run.sh src-to-bckp

# bckphost
17 * * * *   backup DRYRUN=1 /opt/bzfs-backup/bzfs_backup_run.sh bckp-prune
27 * * * *   backup DRYRUN=1 /opt/bzfs-backup/bzfs_backup_run.sh bckp-freshness
37 * * * *   backup DRYRUN=1 /opt/bzfs-backup/bzfs_backup_run.sh arc-freshness
47 3 * * *   backup DRYRUN=1 /opt/bzfs-backup/bzfs_backup_run.sh bckp-bookmarks

# archost
*/15 * * * * backup DRYRUN=1 /opt/bzfs-backup/bzfs_backup_run.sh bckp-to-arc
```

## Alerting

`bzfs_alert.sh <job> <command...>` runs the job, records the outcome in `~/.bzfs-alert/<job>.state`, and notifies via
`bzfs_notify.sh` only when **all three** of the following hold:

1. the job failed at least `min_failures` times in a row, and
2. the current run of failures has lasted at least `grace_secs`, and
3. no alert for this job was sent within the last `renotify_secs`.

When a failing job succeeds again, a single `RECOVERED` notification is sent and the state is reset. Exit code 4
(`bzfs`: "the same previous periodic job has not completed yet") is treated as neither success nor failure, so a slow
transfer that overruns its schedule does not look like an outage.

Defaults, all editable at the top of `bzfs_alert.sh` / `bzfs_alert.py`:

| Job              | Grace | Min failures | Re-notify |
| ---------------- | ----- | ------------ | --------- |
| `src-to-bckp`    | 2 h   | 3            | 12 h      |
| `bckp-prune`     | 6 h   | 3            | 24 h      |
| `bckp-freshness` | 24 h  | 2            | 7 d       |
| `bckp-bookmarks` | 24 h  | 3            | 7 d       |
| `arc-freshness`  | 36 h  | 3            | 24 h      |
| `bckp-to-arc`    | 36 h  | 3            | 24 h      |

The grace period is measured from the **first failure of the current streak**, not from the last success. That
distinction matters: `srchost` may be off for a fortnight, and measuring from the last success would fire an alert on
its very first attempt after coming back, before anything has actually gone wrong.

The two `*-freshness` jobs are the complement to the wrapper: a wrapper can only notice a job that ran and failed, and
never a job that stopped being scheduled at all. Those two run on the always-on `bckphost` and judge the chain by what
is actually stored, via `bzfs --monitor-snapshots`.

### One honest limitation

From `bckphost`, "`srchost` is switched off" and "`srchost` is broken and silent" look identical. There is no way around
that, so the split is:

- **`srchost` failing while it is awake** is caught within a couple of hours by `src-to-bckp`, which is where a real
  push failure actually shows up.
- **`srchost` never coming back** is caught by `bckp-freshness`, deliberately tuned coarse (warning at 7 days, critical
  at 21 days of no new daily snapshot) so that an ordinary holiday does not page you. Tighten `monitor_snapshot_plan` in
  tier 1 if your `srchost` is on every day.

The `archost` side has no such ambiguity: `archost` is expected daily, so `arc-freshness` alerts after 36 hours without
a successful check, whether the cause is an unreachable host or a stale replica.

## Bringing it up

1. Install `bzfs` on all three hosts and put `bzfs` and `bzfs_jobrunner` on the PATH.
2. Set up SSH so that `srchost` can reach `bckphost`, and `archost` can reach `bckphost`, and `bckphost` can reach
   `archost` (the last one only for `arc-freshness`). Give the backup user the ZFS permissions described in the main
   [README](../README.md); `bzfs` uses `sudo` on the remote side unless you delegate via `zfs allow`.
3. Create the destination pools/datasets: `bak` on `bckphost`, `archive` on `archost`.
4. Edit the two jobconfigs as listed above.
5. Dry-run every job by hand, on the host it belongs to, and read the output:
   ```
   /opt/bzfs-backup/bzfs_backup_run.sh src-to-bckp      # DRYRUN defaults to 1
   ```
6. Do the very first replication by hand with both hosts up and `DRYRUN=0`. It is a full send and can take hours;
   subsequent runs are incremental.
7. Verify the alert path end to end: `~/.bzfs-alert/` should fill up, and `bzfs_notify.sh 'test' <<< 'test body'` should
   reach you.
8. Only then switch the timers to `DRYRUN=0`.

To convince yourself the chain is intact, compare the three hosts:

```
bzfs tank/work bckphost:bak/srchost/work --recursive --skip-replication --compare-snapshot-lists=src+dst+all
```
