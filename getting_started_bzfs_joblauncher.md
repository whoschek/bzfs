<!--
 Copyright 2024 Wolfgang Hoschek AT mac DOT com

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.-->

# Getting Started with bzfs_joblauncher

This guide walks you through a minimal, working setup using `bzfs_joblauncher` and a YAML config. We’ll create periodic
snapshots on host A, replicate them to host B using identical destination dataset names, prune them according to policy,
and monitor schedule adherence.

Assumptions:

- Two hosts: A (source) and B (destination), reachable via SSH.
- ZFS installed and pools/datasets available on both ends.
- You can `ssh` from B to A (pull mode recommended) and/or from A to B (push mode) without interactive prompts.

## 1. Install

On both hosts:

```
sudo apt-get -y install zfsutils-linux python3  # or equivalent for your OS
sudo apt-get -y install zstd pv mbuffer         # optional helpers

git clone https://github.com/whoschek/bzfs.git
cd bzfs
python3 -m venv venv                      # Create a Python virtual environment
source venv/bin/activate                  # Activate it
pip install -e '.[joblauncher]'           # Install the YAML parser extra
```

## 2. Create a YAML config

We’ll snapshot and replicate tank1/foo and tank1/bar from A to B into the very same dataset names, every 5 minutes, and
apply this policy:

```
org: prod
target: offsite
policy: {"5minutely": 10, "hourly": 72, "daily": 90}
```

Save the following on both hosts as `/etc/bzfs/bzfs_joblauncher.yaml` (adapt hostnames):

```
root_dataset_pairs:
  - { src: "tank1/foo", dst: "tank1/foo" }
  - { src: "tank1/bar", dst: "tank1/bar" }

recursive: true

# Run on A and B; in each cron entry we subset hosts via --src-host/--dst-host
src_hosts: [ "A" ]

# Destination host: B receives replicas targeted as "offsite"
dst_hosts:
  B: [ "offsite" ]
retain_dst_targets:
  B: [ "offsite" ]

# Replicate into the same dataset names on B
dst_root_datasets:
  B: ""

src_snapshot_plan:
  prod:
    offsite: { "5minutely": 10, hourly: 72, daily: 90 }

# Keep the same plan for bookmarks and destination pruning
src_bookmark_plan:
  prod:
    offsite: { "5minutely": 10, hourly: 72, daily: 90 }

dst_snapshot_plan:
  prod:
    offsite: { "5minutely": 10, hourly: 72, daily: 90 }

# Alert if snapshots are late (warning vs critical); tune to your needs
monitor_snapshot_plan:
  prod:
    offsite:
      "5minutely": { warning: "3 minutes",  critical: "15 minutes" }
      hourly:       { warning: "30 minutes", critical: "120 minutes" }
      daily:        { warning: "3 hours",    critical: "10 hours" }

workers: "100%"
work_period_seconds: 0
jitter: false
extra_args: []
```

Notes:

- Adjust `A` and `B` to your actual hostnames.
- If you prefer push mode or a third-party launcher host, you can still use the same YAML; only the CLI flags change
  (see below).

## 3. Schedule periodic jobs (every 5 minutes)

Using cron as an example (replace paths as appropriate):

On source host A:

```
*/5 * * * * testuser /path/to/bzfs_joblauncher --config=/etc/bzfs/bzfs_joblauncher.yaml \
    --src-host="A" --create-src-snapshots --prune-src-snapshots --prune-src-bookmarks

*/5 * * * * testuser /path/to/bzfs_joblauncher --config=/etc/bzfs/bzfs_joblauncher.yaml \
    --src-host="A" --monitor-src-snapshots
```

On destination host B (pull recommended):

```
*/5 * * * * testuser /path/to/bzfs_joblauncher --config=/etc/bzfs/bzfs_joblauncher.yaml \
    --dst-host="B" --replicate --prune-dst-snapshots

*/5 * * * * testuser /path/to/bzfs_joblauncher --config=/etc/bzfs/bzfs_joblauncher.yaml \
    --dst-host="B" --monitor-dst-snapshots
```

These four entries snapshot (on A), replicate (to B), prune (on A and B), and monitor (both), every five minutes.

## 4. Verify

Manual checks you can run while getting started:

- On A: `zfs list -t snapshot -r tank1/foo tank1/bar` to see new snapshots like `prod_offsite_<timestamp>_5minutely`.
- On B: `zfs list -t snapshot -r tank1/foo tank1/bar` to confirm replicas arrive.
- Run `bzfs_joblauncher --config=... --dst-host=B --replicate --dryrun` via `extra_args` set in YAML to preview actions
  without changes.

Common tips:

- Ensure SSH connectivity between B and A (pull) or A and B (push). Use per-host SSH config if needed.
- If you hit throttling needs, set `workers` to a smaller percentage or integer.

## 5. Monitoring Plan

The `monitor_snapshot_plan` section serves as your monitoring policy. It checks the age of the latest (and oldest)
snapshots for each period and target. A few practical steps:

- Start with generous thresholds (e.g., `5minutely`: warning at 3 minutes late, critical at 15 minutes), then tighten
  later.
- Add alerts for `hourly` and `daily` as shown to cover coarser aggregations.
- Wire the exit status (0 OK, 1 WARNING, 2 CRITICAL) into your monitoring stack:
  - systemd: use `OnFailure=` units
  - cron + Monit: monitor the exit code and log files
  - Prometheus: scrape a wrapper that emits a metric based on the exit code

Operational checks:

- Snapshots successfully taken on schedule: `--monitor-src-snapshots` should remain green. If it alerts, confirm
  `--create-src-snapshots` runs and that `src_snapshot_plan` matches expectations.
- Replication successfully on schedule: `--monitor-dst-snapshots` should remain green. If it alerts, check SSH, network
  bandwidth, and target mappings in `dst_hosts`.
- Pruning successfully on schedule: With `--prune-src-*` and `--prune-dst-snapshots` enabled, the number of retained
  snapshots should match your plans.

With this in place, you should have a robust, simple setup that snapshots, replicates, prunes, and monitors every five
minutes. Tweak YAML values as your environment grows. For advanced tuning (daemon mode, recv options,
compression-on-the-wire, etc.), set those in `extra_args` and consult `README.md` and `README_bzfs_jobrunner.md`.
