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

This program is a convenience wrapper around [bzfs](README.md) that simplifies periodic ZFS
snapshot creation, replication, pruning, and monitoring, across source host and multiple
destination hosts, using a single shared [jobconfig](bzfs_tests/bzfs_job_example.py) script.

Typically, a cron job on the source host runs `bzfs_jobrunner` periodically to create new
snapshots (via --create-src-snapshots) and prune outdated snapshots and bookmarks on the source
(via --prune-src-snapshots and --prune-src-bookmarks), whereas another cron job on the
destination host runs `bzfs_jobrunner` periodically to prune outdated destination snapshots (via
--prune-dst-snapshots), and to replicate the recently created snapshots from the source to the
destination (via --replicate). Yet another cron job on both source and destination runs
`bzfs_jobrunner` periodically to alert the user if the latest or oldest snapshot is somehow too
old (via --monitor-src-snapshots and --monitor-dst-snapshots). The frequency of these periodic
activities can vary by activity, and is typically every second, minute, hour, day, week, month
and/or year (or multiples thereof).

Edit the jobconfig script in a central place (e.g. versioned in a git repo), then copy the (very
same) shared file onto the source host and all destination hosts, and add crontab entries (or
systemd timers or Monit entries or similar), along these lines:

* crontab on source host:

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --create-src-snapshots
--prune-src-snapshots --prune-src-bookmarks`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --monitor-src-snapshots`

* crontab on destination host(s):

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --replicate=pull --prune-dst-snapshots`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --monitor-dst-snapshots`

### High Frequency Replication (Experimental Feature)

Taking snapshots, and/or replicating, from every N milliseconds to every 10 seconds or so is
considered high frequency. For such use cases, consider that `zfs list -t snapshot` performance
degrades as more and more snapshots currently exist within the selected datasets, so try to keep
the number of currently existing snapshots small, and prune them at a frequency that is
proportional to the frequency with which snapshots are created. Consider using `--skip-parent`
and `--exclude-dataset*` filters to limit the selected datasets only to those that require
this level of frequency.

In addition, use the `--daemon-*` options to reduce startup overhead, in combination with
splitting the crontab entry (or better: high frequency systemd timer) into multiple processes,
using pull replication mode, along these lines:

* crontab on source host:

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --create-src-snapshots`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --prune-src-snapshots`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --prune-src-bookmarks`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --monitor-src-snapshots`

* crontab on destination host(s):

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --replicate=pull`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --prune-dst-snapshots`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --monitor-dst-snapshots`

The daemon processes work like non-daemon processes except that they loop, handle time events and
sleep between events, and finally exit after, say, 86400 seconds (whatever you specify via
`--daemon-lifetime`). The daemons will subsequently be auto-restarted by 'cron', or earlier
if they fail. While the daemons are running, 'cron' will attempt to start new (unnecessary)
daemons but this is benign as these new processes immediately exit with a message like this:
"Exiting as same previous periodic job is still running without completion yet"

<!-- END DESCRIPTION SECTION -->


<!-- FINE TO EDIT -->

# Man Page

<!-- DO NOT EDIT (auto-generated from ArgumentParser help text as the source of "truth", via update_readme.py) -->
<!-- BEGIN HELP OVERVIEW SECTION -->
```
usage: bzfs_jobrunner [-h] [--create-src-snapshots]
                      [--replicate [{pull,push}]]
                      [--prune-src-snapshots] [--prune-src-bookmarks]
                      [--prune-dst-snapshots]
                      [--monitor-src-snapshots]
                      [--monitor-dst-snapshots] [--src-host STRING]
                      [--localhost STRING] [--dst-hosts DICT_STRING]
                      [--retain-dst-targets DICT_STRING]
                      [--dst-root-datasets DICT_STRING]
                      [--src-snapshot-plan DICT_STRING]
                      [--src-bookmark-plan DICT_STRING]
                      [--dst-snapshot-plan DICT_STRING]
                      [--monitor-snapshot-plan DICT_STRING]
                      [--src-user STRING] [--dst-user STRING]
                      [--job-id STRING]
                      [--daemon-replication-frequency STRING]
                      [--daemon-prune-src-frequency STRING]
                      [--daemon-prune-dst-frequency STRING]
                      [--daemon-monitor-snapshots-frequency STRING]
                      --root-dataset-pairs SRC_DATASET DST_DATASET
                      [SRC_DATASET DST_DATASET ...]
```
<!-- END HELP OVERVIEW SECTION -->

<!-- BEGIN HELP DETAIL SECTION -->
<div id="--create-src-snapshots"></div>

**--create-src-snapshots**

*  Take snapshots on src as necessary. This command should be called by a program (or cron job)
    running on the src host.

<!-- -->

<div id="--replicate"></div>

**--replicate** *[{pull,push}]*

*  Replicate snapshots from src to dst as necessary, either in pull mode (recommended) or push
    mode (experimental). For pull mode, this command should be called by a program (or cron job)
    running on the dst host; for push mode, on the src host.

<!-- -->

<div id="--prune-src-snapshots"></div>

**--prune-src-snapshots**

*  Prune snapshots on src as necessary. This command should be called by a program (or cron job)
    running on the src host.

<!-- -->

<div id="--prune-src-bookmarks"></div>

**--prune-src-bookmarks**

*  Prune bookmarks on src as necessary. This command should be called by a program (or cron job)
    running on the src host.

<!-- -->

<div id="--prune-dst-snapshots"></div>

**--prune-dst-snapshots**

*  Prune snapshots on dst as necessary. This command should be called by a program (or cron job)
    running on the dst host.

<!-- -->

<div id="--monitor-src-snapshots"></div>

**--monitor-src-snapshots**

*  Alert the user if src snapshots are too old, using --monitor-snapshot-plan (see below). This
    command should be called by a program (or cron job) running on the src host.

<!-- -->

<div id="--monitor-dst-snapshots"></div>

**--monitor-dst-snapshots**

*  Alert the user if dst snapshots are too old, using --monitor-snapshot-plan (see below). This
    command should be called by a program (or cron job) running on the dst host.

<!-- -->

<div id="--src-host"></div>

**--src-host** *STRING*

*  Network hostname of src. Used by destination host(s) if replicating in pull mode.

<!-- -->

<div id="--localhost"></div>

**--localhost** *STRING*

*  Hostname of localhost. Default is the hostname without the domain name.

<!-- -->

<div id="--dst-hosts"></div>

**--dst-hosts** *DICT_STRING*

*  Dictionary that maps each destination hostname to a list of zero or more logical replication
    target names (the infix portion of snapshot name). Example: `"{'nas': ['onsite'],
    'bak-us-west-1': ['us-west-1'], 'bak-eu-west-1': ['eu-west-1'], 'archive':
    ['offsite']}"`. With this, given a snapshot name, we can find the destination hostname
    to which the snapshot shall be replicated. Also, given a snapshot name and its --localhost
    name, a destination host can determine if it shall 'pull' replicate the given snapshot from
    the --src-host, or if the snapshot is intended for another destination host, in which case it
    skips the snapshot. A destination host running bzfs_jobrunner will 'pull' snapshots for all
    targets that map to its --localhost name.

<!-- -->

<div id="--retain-dst-targets"></div>

**--retain-dst-targets** *DICT_STRING*

*  Dictionary that maps each destination hostname to a list of zero or more logical replication
    target names (the infix portion of snapshot name). Example: `"{'nas': ['onsite'],
    'bak-us-west-1': ['us-west-1'], 'bak-eu-west-1': ['eu-west-1'], 'archive':
    ['offsite']}"`. Has same format as --dst-hosts. As part of --prune-dst-snapshots, a
    destination host will delete any snapshot it has stored whose target has no mapping to its
    --localhost name in this dictionary. Do not remove a mapping unless you are sure it's ok to
    delete all those snapshots on that destination host! If in doubt, use --dryrun mode first.

<!-- -->

<div id="--dst-root-datasets"></div>

**--dst-root-datasets** *DICT_STRING*

*  Dictionary that maps each destination hostname to a root dataset located on that destination
    host. The root dataset name is an (optional) prefix that will be prepended to each dataset
    that is replicated to that destination host. For backup use cases, this is the backup ZFS pool
    or a ZFS dataset path within that pool, whereas for cloning, master slave replication, or
    replication from a primary to a secondary, this can also be the empty string. Example:
    `"{'nas': 'tank2/bak', 'bak-us-west-1': 'backups/bak001', 'bak-eu-west-1':
    'backups/bak999', 'archive': 'f"archives/zoo/{src_host}"', 'hotspare': ''}"`

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

<div id="--monitor-snapshot-plan"></div>

**--monitor-snapshot-plan** *DICT_STRING*

*  Alert the user if the ZFS 'creation' time property of the latest or oldest snapshot for any
    specified snapshot pattern within the selected datasets is too old wrt. the specified age
    limit. The purpose is to check if snapshots are successfully taken on schedule, successfully
    replicated on schedule, and successfully pruned on schedule. Process exit code is 0, 1, 2 on
    OK, WARNING, CRITICAL, respectively. Example DICT_STRING: `"{'prod': {'onsite':
    {'100millisecondly': {'warning': '650 milliseconds', 'critical': '2 seconds'},
    'secondly': {'warning': '2 seconds', 'critical': '14 seconds'}, 'minutely':
    {'warning': '30 seconds', 'critical': '300 seconds'}, 'hourly': {'warning': '30
    minutes', 'critical': '300 minutes'}, 'daily': {'warning': '4 hours', 'critical':
    '8 hours'}, 'weekly': {'warning': '2 days', 'critical': '8 days'}, 'monthly':
    {'warning': '2 days', 'critical': '8 days'}, 'yearly': {'warning': '5 days',
    'critical': '14 days'}, '10minutely': {'warning': '0 minutes', 'critical': '0
    minutes'}}, '': {'daily': {'warning': '4 hours', 'critical': '8 hours'}}}}"`.
    This example alerts the user if the latest src or dst snapshot named
    `prod_onsite_<timestamp>_hourly` is more than 30 minutes late (i.e. more than 30+60=90
    minutes old) [warning] or more than 300 minutes late (i.e. more than 300+60=360 minutes old)
    [critical]. In addition, the example alerts the user if the oldest src or dst snapshot named
    `prod_onsite_<timestamp>_hourly` is more than 30 + 60x36 minutes old [warning] or more
    than 300 + 60x36 minutes old [critical], where 36 is the number of period cycles specified
    in `src_snapshot_plan` or `dst_snapshot_plan`, respectively. Analog for the latest
    snapshot named `prod_<timestamp>_daily`, and so on.

    Note: A duration that is missing or zero (e.g. '0 minutes') indicates that no snapshots
    shall be checked for the given snapshot name pattern.

<!-- -->

<div id="--src-user"></div>

**--src-user** *STRING*

*  SSH username on --src-host. Used if replicating in pull mode. Example: 'alice'

<!-- -->

<div id="--dst-user"></div>

**--dst-user** *STRING*

*  SSH username on dst. Used if replicating in push mode. Example: 'root'

<!-- -->

<div id="--job-id"></div>

**--job-id** *STRING*

*  The job identifier that shall be included in the log file name suffix. Default is a hex UUID.
    Example: 0badc0f003a011f0a94aef02ac16083c

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

<!-- -->

<div id="--daemon-monitor-snapshots-frequency"></div>

**--daemon-monitor-snapshots-frequency** *STRING*

*  Specifies how often the bzfs daemon shall monitor snapshot age if --daemon-lifetime is
    nonzero.

<!-- -->

<div id="--root-dataset-pairs"></div>

**--root-dataset-pairs** *SRC_DATASET DST_DATASET [SRC_DATASET DST_DATASET ...]*

*  Source and destination dataset pairs (excluding usernames and excluding hostnames, which will
    all be auto-appended later).
