<!--
 Copyright 2024 Wolfgang Hoschek AT mac DOT com

 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Periodic Jobs with bzfs_jobrunner

- [Introduction](#Introduction)
- [Man Page](#Man-Page)

# Introduction

<!-- DO NOT EDIT (auto-generated from ArgumentParser help text as the source of "truth", via update_readme.py) -->
<!-- BEGIN DESCRIPTION SECTION -->
WARNING: For now, `bzfs_jobrunner` is work-in-progress, and as such may still change in
incompatible ways.

This program is a convenience wrapper around [bzfs](README.md) that simplifies periodic snapshot
creation, replication, and pruning, across multiple source and destination hosts, using a single
shared [jobconfig](bzfs_tests/bzfs_job_example.py) file.

Typically, a cron job on the source host runs `bzfs_jobrunner` periodically to create new
snapshots (via --create-src-snapshots) and prune outdated snapshots and bookmarks on the source
(via --prune-src-snapshots and --prune-src-bookmarks), whereas another cron job on the
destination host runs `bzfs_jobrunner` periodically to prune outdated destination snapshots (via
--prune-dst-snapshots). Yet another cron job runs `bzfs_jobrunner` periodically to replicate
the recently created snapshots from the source to the destination (via --replicate). The
frequency of these periodic activities can vary by activity, and is typically every second,
minute, hour, day, week, month and/or year (or multiples thereof).

Edit the jobconfig file in a central place (e.g. versioned in a git repo), then copy the (very
same) shared file onto the source host and all destination hosts, and add crontab entries or
systemd timers or similar, along these lines:

* crontab on source host:

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --create-src-snapshots
--prune-src-snapshots --prune-src-bookmarks`

* crontab on destination host(s):

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --replicate --prune-dst-snapshots`

### High Frequency Replication (Experimental Feature)

Taking snapshots, and/or replicating, from every N milliseconds to every 10 seconds or so is
considered high frequency. For such use cases, consider that `zfs list -t snapshot` performance
degrades as more and more snapshots currently exist within the selected datasets, so try to keep
the number of currently existing snapshots small, and prune them at a frequency that is
proportional to the frequency with which snapshots are created. Consider using `--skip-parent`
and `--exclude-dataset*` filters to limit the selected datasets only to those that require
this level of frequency. If running with `--no-privilege-elevation` also set Unix env var
`bzfs_no_force_convert_I_to_i` to `true` to enable batched incremental replication (requires
permission for ZFS holds on the source dataset via `zfs allow`).

In addition, use the `--daemon-*` options to reduce startup overhead, in combination with
splitting the crontab entry (or better: high frequency systemd timer) into multiple processes,
using pull replication mode, along these lines:

* crontab on source host:

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --create-src-snapshots`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --prune-src-snapshots`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --prune-src-bookmarks`

* crontab on destination host(s):

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --replicate`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --prune-dst-snapshots`

The daemon processes work like non-daemon processes except that they loop, handle time events and
sleep between events, and finally exit after, say, 86400 seconds (whatever you specify via
`--daemon-lifetime`). The daemons will subsequently be auto-restarted by 'cron', or earlier
if they fail. While the daemons are running 'cron' will attempt to start new (unnecessary)
daemons but this is benign as these new processes immediately exit with a message like this:
"Exiting as same previous periodic job is still running without completion yet"

<!-- END DESCRIPTION SECTION -->


<!-- FINE TO EDIT -->

# Man Page

<!-- DO NOT EDIT (auto-generated from ArgumentParser help text as the source of "truth", via update_readme.py) -->
<!-- BEGIN HELP OVERVIEW SECTION -->
```
usage: bzfs_jobrunner [-h] [--create-src-snapshots] [--replicate]
                      [--prune-src-snapshots] [--prune-src-bookmarks]
                      [--prune-dst-snapshots] [--src-host STRING]
                      [--localhost STRING] [--dst-hosts DICT_STRING]
                      [--retain-dst-targets DICT_STRING]
                      [--dst-root-datasets DICT_STRING]
                      [--src-snapshot-plan DICT_STRING]
                      [--src-bookmark-plan DICT_STRING]
                      [--dst-snapshot-plan DICT_STRING]
                      [--src-user STRING] [--dst-user STRING]
                      [--daemon-replication-frequency STRING]
                      [--daemon-prune-src-frequency STRING]
                      [--daemon-prune-dst-frequency STRING]
                      SRC_DATASET DST_DATASET
                      [SRC_DATASET DST_DATASET ...]
```
<!-- END HELP OVERVIEW SECTION -->

<!-- BEGIN HELP DETAIL SECTION -->
<div id="SRC_DATASET_DST_DATASET"></div>

**SRC_DATASET DST_DATASET**

*  Source and destination dataset pairs (excluding usernames and excluding hostnames, which will
    all be auto-appended later).



<div id="--create-src-snapshots"></div>

**--create-src-snapshots**

*  Take snapshots on src. This command should be called by a program (or cron job) running on the
    src host.

<!-- -->

<div id="--replicate"></div>

**--replicate**

*  Replicate recent snapshots from src to dst, either in pull mode (recommended) or push mode.
    For pull mode, this command should be called by a program (or cron job) running on the dst
    host; for push mode on the src host.

<!-- -->

<div id="--prune-src-snapshots"></div>

**--prune-src-snapshots**

*  Prune snapshots on src. This command should be called by a program (or cron job) running on
    the src host.

<!-- -->

<div id="--prune-src-bookmarks"></div>

**--prune-src-bookmarks**

*  Prune bookmarks on src. This command should be called by a program (or cron job) running on
    the src host.

<!-- -->

<div id="--prune-dst-snapshots"></div>

**--prune-dst-snapshots**

*  Prune snapshots on dst. This command should be called by a program (or cron job) running on
    the dst host.

<!-- -->

<div id="--src-host"></div>

**--src-host** *STRING*

*  Network hostname of src. Used if replicating in pull mode.

<!-- -->

<div id="--localhost"></div>

**--localhost** *STRING*

*  Hostname of localhost. Default is the hostname without the domain name.

<!-- -->

<div id="--dst-hosts"></div>

**--dst-hosts** *DICT_STRING*

*  Dictionary that maps logical replication target names (the infix portion of a snapshot name)
    to destination hostnames. Example: `"{'onsite': 'nas', 'us-west-1': 'bak-us-west-1',
    'eu-west-1': 'bak-eu-west-1', 'offsite': 'archive'}"`. With this, given a snapshot
    name, we can find the destination hostname to which the snapshot shall be replicated. Also,
    given a snapshot name and its --localhost name, a destination host can determine if it shall
    'pull' replicate the given snapshot from the --src-host, or if the snapshot is intended for
    another target host, in which case it skips the snapshot. A destination host running
    bzfs_jobrunner will 'pull' snapshots for all targets that map to its --localhost name.

<!-- -->

<div id="--retain-dst-targets"></div>

**--retain-dst-targets** *DICT_STRING*

*  Dictionary that maps logical replication target names (the infix portion of a snapshot name)
    to destination hostnames. Example: `"{'onsite': 'nas', 'us-west-1': 'bak-us-west-1',
    'eu-west-1': 'bak-eu-west-1', 'offsite': 'archive'}"`. As part of
    --prune-dst-snapshots, a destination host will delete any snapshot it has stored whose target
    has no mapping to its --localhost name in this dictionary. Do not remove a mapping unless you
    are sure it's ok to delete all those snapshots!

<!-- -->

<div id="--dst-root-datasets"></div>

**--dst-root-datasets** *DICT_STRING*

*  Dictionary that maps each destination hostname to a root dataset located on that destination
    host. The root dataset name is an (optional) prefix that will be prepended to each dataset
    that is replicated to that destination host. For backup use cases, this is the backup ZFS pool
    or a ZFS dataset path within that pool, whereas for cloning, master slave replication, or
    replication from a primary to a secondary, this can also be the empty string. Example:
    `"{'nas': 'tank2/bak', 'bak-us-west-1': 'backups/bak001', 'bak-eu-west-1':
    'backups/bak999', 'archive': 'archives/zoo', 'hotspare': ''}"`

<!-- -->

<div id="--src-snapshot-plan"></div>

**--src-snapshot-plan** *DICT_STRING*

*  Retention periods for snapshots to be used if pruning src, and when creating new snapshots on
    src. Snapshots that do not match a retention period will be deleted. A zero within a retention
    period indicates that no snapshots shall be retained (or even be created) for the given
    period.

    Example: `"{'prod': {'onsite': {'secondly': 40, 'minutely': 40, 'hourly': 36,
    'daily': 31, 'weekly': 12, 'monthly': 18, 'yearly': 5}, 'us-west-1': {'secondly':
    0, 'minutely': 0, 'hourly': 36, 'daily': 31, 'weekly': 12, 'monthly': 18,
    'yearly': 5}, 'eu-west-1': {'secondly': 0, 'minutely': 0, 'hourly': 36, 'daily':
    31, 'weekly': 12, 'monthly': 18, 'yearly': 5}}, 'test': {'offsite': {'12hourly':
    42, 'weekly': 12}}}"`. This example will, for the organization 'prod' and the intended
    logical target 'onsite', create and then retain secondly snapshots that were created less
    than 40 seconds ago, yet retain the latest 40 secondly snapshots regardless of creation time.
    Analog for the latest 40 minutely snapshots, 36 hourly snapshots, etc. It will also create and
    retain snapshots for the targets 'us-west-1' and 'eu-west-1' within the 'prod'
    organization. In addition, it will create and retain snapshots every 12 hours and every week
    for the 'test' organization, and name them as being intended for the 'offsite' replication
    target. The example creates snapshots with names like
    `prod_onsite_<timestamp>_secondly`, `prod_onsite_<timestamp>_minutely`,
    `prod_us-west-1_<timestamp>_hourly`, `prod_us-west-1_<timestamp>_daily`,
    `prod_eu-west-1_<timestamp>_hourly`, `prod_eu-west-1_<timestamp>_daily`,
    `test_offsite_<timestamp>_12hourly`, `test_offsite_<timestamp>_weekly`, and so on.

<!-- -->

<div id="--src-bookmark-plan"></div>

**--src-bookmark-plan** *DICT_STRING*

*  Retention periods for bookmarks to be used if pruning src. Has same format as
    --src-snapshot-plan.

<!-- -->

<div id="--dst-snapshot-plan"></div>

**--dst-snapshot-plan** *DICT_STRING*

*  Retention periods for snapshots to be used if pruning dst. Has same format as
    --src-snapshot-plan.

<!-- -->

<div id="--src-user"></div>

**--src-user** *STRING*

*  SSH username on --src-host. Used if replicating in pull mode.

<!-- -->

<div id="--dst-user"></div>

**--dst-user** *STRING*

*  SSH username on dst. Used if replicating in push mode.

<!-- -->

<div id="--daemon-replication-frequency"></div>

**--daemon-replication-frequency** *STRING*

*  Specifies how often the bzfs daemon shall replicate from src to dst if --daemon-lifetime is
    nonzero.

<!-- -->

<div id="--daemon-prune-src-frequency"></div>

**--daemon-prune-src-frequency** *STRING*

*  Specifies how often the bzfs daemon shall prune src if --daemon-lifetime is nonzero.

<!-- -->

<div id="--daemon-prune-dst-frequency"></div>

**--daemon-prune-dst-frequency** *STRING*

*  Specifies how often the bzfs daemon shall prune dst if --daemon-lifetime is nonzero.
