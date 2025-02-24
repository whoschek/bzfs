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

# bzfs_cron

<!-- DO NOT EDIT (auto-generated from ArgumentParser help text as the source of "truth", via update_readme.py) -->
<!-- BEGIN DESCRIPTION SECTION -->
WARNING: For now, `bzfs_cron` is work-in-progress, and as such may still change in incompatible
ways.

This program is a convenience wrapper around [bzfs](README.md) that automates periodic
activities such as creating snapshots, replicating and pruning, on multiple source hosts and
multiple destination hosts, using a single shared [deployment specification
file](bzfs_tests/bzfs_cron_example.py).

Typically, a cron job on the source host runs `bzfs_cron` periodically to create new snapshots
(via --create-src-snapshots) and prune outdated snapshots and bookmarks on the source (via
--prune-src-snapshots and --prune-src-bookmarks), whereas another cron job on the destination
host runs `bzfs_cron` periodically to prune outdated destination snapshots (via
--prune-dst-snapshots). Yet another cron job runs `bzfs_cron` periodically to replicate the
recently created snapshots from the source to the destination (via --replicate). The frequency of
these periodic activities can vary by activity, and is typically every second, minute, hour, day,
week, month and/or year (or multiples thereof).

This tool is just a convenience wrapper around the `bzfs` CLI.

<!-- END DESCRIPTION SECTION -->


<!-- FINE TO EDIT -->

# Usage

<!-- DO NOT EDIT (auto-generated from ArgumentParser help text as the source of "truth", via update_readme.py) -->
<!-- BEGIN HELP OVERVIEW SECTION -->
```
usage: bzfs_cron [-h] [--create-src-snapshots] [--replicate]
                 [--prune-src-snapshots] [--prune-src-bookmarks]
                 [--prune-dst-snapshots] [--src-host STRING]
                 [--dst-hosts DICT_STRING]
                 [--src-snapshot-periods DICT_STRING]
                 [--src-bookmark-periods DICT_STRING]
                 [--dst-snapshot-periods DICT_STRING]
                 [--src-user STRING] [--dst-user STRING]
                 [--daemon-replication-frequency STRING]
                 [--daemon-prune-src-frequency STRING]
                 [--daemon-prune-dst-frequency STRING]
                 SRC_DATASET DST_DATASET [SRC_DATASET DST_DATASET ...]
```
<!-- END HELP OVERVIEW SECTION -->

<!-- BEGIN HELP DETAIL SECTION -->
<div id="SRC_DATASET_DST_DATASET"></div>

**SRC_DATASET DST_DATASET**

*  Source and destination dataset pairs (excluding usernames and excluding host names, which will
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

*  Network host name of src. Used if replicating in pull mode.

<!-- -->

<div id="--dst-hosts"></div>

**--dst-hosts** *DICT_STRING*

*  Dictionary that maps logical replication target names (the infix portion of a snapshot name)
    to actual destination network host names. Example: `"{'onsite': 'nas', 'us-west-1':
    'bak-us-west-1.example.com', 'eu-west-1': 'bak-eu-west-1.example.com', 'offsite':
    'archive.example.com'}"`. With this, given a snapshot name, we can find the destination
    network host name to which the snapshot shall be replicated. Also, given a snapshot name and
    its own hostname, a destination host can determine if it shall 'pull' replicate the given
    snapshot from the --src-host, or if the snapshot is intended for another target host, in
    which case it skips the snapshot. A destination host running bzfs_cron will 'pull' snapshots
    for all targets that map to its own hostname.

<!-- -->

<div id="--src-snapshot-periods"></div>

**--src-snapshot-periods** *DICT_STRING*

*  Retention periods for snapshots to be used if pruning src, and when creating new snapshots on
    src. Snapshots that do not match a retention period will be deleted. A zero within a retention
    period indicates that no snapshots shall be retained (or even be created) for the given
    period.

    Example: `"{'prod': {'onsite': {'secondly': 150, 'minutely': 90, 'hourly': 48,
    'daily': 31, 'weekly': 26, 'monthly': 18, 'yearly': 5}, 'us-west-1': {'secondly':
    0, 'minutely': 0, 'hourly': 48, 'daily': 31, 'weekly': 26, 'monthly': 18,
    'yearly': 5}, 'eu-west-1': {'secondly': 0, 'minutely': 0, 'hourly': 48, 'daily':
    31, 'weekly': 26, 'monthly': 18, 'yearly': 5}}, 'test': {'offsite': {'12hourly':
    42, 'weekly': 12}}}"`. This example will, for the organization 'prod' and the intended
    logical target 'onsite', create and then retain secondly snapshots that were created less
    than 150 seconds ago, yet retain the latest 150 secondly snapshots regardless of creation
    time. Analog for the latest 90 minutely snapshots, 48 hourly snapshots, etc. It will also
    create and retain snapshots for the targets 'us-west-1' and 'eu-west-1' within the
    'prod' organization. In addition, it will create and retain snapshots every 12 hours and
    every week for the 'test' organization, and name them as being intended for the 'offsite'
    replication target. The example creates snapshots with names like
    `prod_<timestamp>_onsite_secondly`, `prod_<timestamp>_onsite_minutely`,
    `prod_<timestamp>_us-west-1_hourly`, `prod_<timestamp>_us-west-1_daily`,
    `prod_<timestamp>_eu-west-1_hourly`, `prod_<timestamp>_eu-west-1_daily`,
    `test_<timestamp>_offsite_12hourly`, `test_<timestamp>_offsite_weekly`, and so on.

<!-- -->

<div id="--src-bookmark-periods"></div>

**--src-bookmark-periods** *DICT_STRING*

*  Retention periods for bookmarks to be used if pruning src. Has same format as
    --src-snapshot-periods.

<!-- -->

<div id="--dst-snapshot-periods"></div>

**--dst-snapshot-periods** *DICT_STRING*

*  Retention periods for snapshots to be used if pruning dst. Has same format as
    --src-snapshot-periods.

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
