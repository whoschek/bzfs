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
# Periodic Jobs with bzfs_jobrunner
- [Introduction](#Introduction)
- [Man Page](#Man-Page)
# Introduction
<!-- DO NOT EDIT (This section was auto-generated from ArgumentParser help text as the source of "truth", via update_readme.sh) -->
<!-- BEGIN-MANPAGE-DESCRIPTION -->
This companion program wraps [bzfs](README.md) for periodic snapshot creation, replication,
pruning, and monitoring across N source hosts and M destination hosts, using one shared fleet-wide
[jobconfig](bzfs_testbed/bzfs_job_testbed.py) script.

Typical use cases include geo-replicated backup where each destination host is in a different
region and receives replicas from the same set of source hosts, low-latency replication from a
primary to a secondary or to M read replicas, and backups to removable drives.

This program can be used to efficiently replicate ...

a) within a single machine (local mode), or

b) from a single source host to one or more destination hosts (pull or push or pull-push mode), or

c) from multiple source hosts to a single destination host (pull or push or pull-push mode), or

d) from N source hosts to M destination hosts (pull or push or pull-push mode, N and M can be
large, M=2 or M=3 are typical geo-replication factors)

You can run this program on a single third-party host and have it talk to all source and
destination hosts. That setup is convenient for basic use cases and testing, and efficient with
`--r2r=pull` or `--r2r=push`. In many deployments, a cron job on each source host runs
`bzfs_jobrunner` periodically to create new snapshots (via --create-src-snapshots) and prune
outdated snapshots and bookmarks on the source (via --prune-src-snapshots and
--prune-src-bookmarks), whereas another cron job on each destination host runs `bzfs_jobrunner`
periodically to prune outdated destination snapshots (via --prune-dst-snapshots), and to replicate
the recently created snapshots from the source to the destination (via --replicate). A separate
cron job on each source host and each destination host runs `bzfs_jobrunner` periodically to alert
the user if the latest or oldest snapshot is somehow too old (via --monitor-src-snapshots and
--monitor-dst-snapshots). The frequency of each activity can differ. Typical intervals range from
N milliseconds to years.

Edit the jobconfig script in a central place (e.g. versioned in a git repo), then copy the (very
same) shared file onto all source hosts and all destination hosts, and add crontab entries (or
systemd timers or Monit entries or similar), along these lines:

* crontab on source hosts:

`* * * * * testuser /bzfs/bzfs_testbed/bzfs_job_testbed.py --src-host="$(hostname)"
--create-src-snapshots --prune-src-snapshots --prune-src-bookmarks`

`* * * * * testuser /bzfs/bzfs_testbed/bzfs_job_testbed.py --src-host="$(hostname)"
--monitor-src-snapshots`

* crontab on destination hosts:

`* * * * * testuser /bzfs/bzfs_testbed/bzfs_job_testbed.py --dst-host="$(hostname)" --replicate
--prune-dst-snapshots`

`* * * * * testuser /bzfs/bzfs_testbed/bzfs_job_testbed.py --dst-host="$(hostname)"
--monitor-dst-snapshots`

Some deployments choose to move monitoring to one centralized management host, like so:

`* * * * * testuser /bzfs/bzfs_testbed/bzfs_job_testbed.py --monitor-src-snapshots
--monitor-dst-snapshots`

### Applying Actions to a Subset of Hosts

`--src-host` and `--dst-host` let you run actions on only a subset of source and destination
hosts. For example, you can replicate from selected source hosts to selected destination hosts.
Each `bzfs_jobrunner` invocation runs all enabled actions for the final effective values of
`--src-hosts` and `--dst-hosts`, after applying the `--src-host` and `--dst-host` filters.

### High Frequency Replication (Experimental Feature)

Taking snapshots and/or replicating every N milliseconds to roughly every 10 seconds is considered
high frequency. Consider that `zfs list -t snapshot` performance degrades as snapshot counts grow
within the selected datasets. Keep the active snapshot count small, and prune at a cadence that
matches your snapshot creation rate. Consider using `--skip-parent` and `--exclude-dataset*`
filters so only datasets that need this frequency are selected.

To reduce startup overhead, forward `--daemon-lifetime` to `bzfs`, use the `--daemon-*` options,
and split the crontab entry (or, preferably, a high-frequency systemd timer) into multiple
processes, from one source host to one destination host, along these lines:

* crontab on source hosts:

`* * * * * testuser /bzfs/bzfs_testbed/bzfs_job_testbed.py --src-host="$(hostname)"
--dst-host="foo" --create-src-snapshots`

`* * * * * testuser /bzfs/bzfs_testbed/bzfs_job_testbed.py --src-host="$(hostname)"
--dst-host="foo" --prune-src-snapshots`

`* * * * * testuser /bzfs/bzfs_testbed/bzfs_job_testbed.py --src-host="$(hostname)"
--dst-host="foo" --prune-src-bookmarks`

`* * * * * testuser /bzfs/bzfs_testbed/bzfs_job_testbed.py --src-host="$(hostname)"
--dst-host="foo" --monitor-src-snapshots`

* crontab on destination hosts:

`* * * * * testuser /bzfs/bzfs_testbed/bzfs_job_testbed.py --src-host="bar"
--dst-host="$(hostname)" --replicate`

`* * * * * testuser /bzfs/bzfs_testbed/bzfs_job_testbed.py --src-host="bar"
--dst-host="$(hostname)" --prune-dst-snapshots`

`* * * * * testuser /bzfs/bzfs_testbed/bzfs_job_testbed.py --src-host="bar"
--dst-host="$(hostname)" --monitor-dst-snapshots`

The daemon processes work like non-daemon processes except that they loop, handle time events, and
sleep between events. They exit after the interval specified by `--daemon-lifetime` (for example,
86400 seconds). They are then restarted by `cron`, or earlier if they fail. While an existing
daemon is still running, `cron` may attempt to start another one. This is harmless because that
extra process exits immediately with a message like this: "Exiting as same previous periodic job
is still running without completion yet"
<!-- END-MANPAGE-DESCRIPTION -->
<!-- FINE TO EDIT -->
# Man Page
<!-- DO NOT EDIT (This section was auto-generated from ArgumentParser help text as the source of "truth", via update_readme.sh) -->
<!-- BEGIN-MANPAGE-USAGE -->
```
usage: bzfs_jobrunner
       [-h]
       [--create-src-snapshots]
       [--replicate]
       [--prune-src-snapshots]
       [--prune-src-bookmarks]
       [--prune-dst-snapshots]
       [--monitor-src-snapshots]
       [--monitor-dst-snapshots]
       [--localhost STRING]
       [--src-hosts LIST_STRING]
       [--src-host STRING]
       [--dst-hosts DICT_STRING]
       [--dst-host STRING]
       [--retain-dst-targets DICT_STRING]
       [--dst-root-datasets DICT_STRING]
       [--src-snapshot-plan DICT_STRING]
       [--src-bookmark-plan DICT_STRING]
       [--dst-snapshot-plan DICT_STRING]
       [--monitor-snapshot-plan DICT_STRING]
       [--ssh-src-user STRING]
       [--ssh-dst-user STRING]
       [--ssh-src-port INT]
       [--ssh-dst-port INT]
       [--ssh-src-config-file FILE]
       [--ssh-dst-config-file FILE]
       --job-id STRING
       [--job-run STRING]
       [--workers INT[%]]
       [--work-period-seconds FLOAT]
       [--jitter]
       [--worker-timeout-seconds FLOAT]
       [--repeat-if-took-more-than-seconds FLOAT]
       [--spawn-process-per-job]
       [--jobrunner-dryrun]
       [--jobrunner-log-level {CRITICAL,ERROR,WARN,INFO,DEBUG,TRACE}]
       [--daemon-replication-frequency STRING]
       [--daemon-prune-src-frequency STRING]
       [--daemon-prune-dst-frequency STRING]
       [--daemon-monitor-snapshots-frequency STRING]
       [--version]
       --root-dataset-pairs SRC_DATASET DST_DATASET [SRC_DATASET DST_DATASET ...]
```
<!-- END-MANPAGE-USAGE -->

<!-- BEGIN-MANPAGE-DETAILS -->
<span id="-h" class="man-option-title">**-h**, **--help**</span> <a href="#-h" title="Permalink to -h" aria-label="Permalink to -h" class="man-option-permalink">&#x1F517;</a>

- show this help message and exit

<!-- -->

<span id="--create-src-snapshots" class="man-option-title">**--create-src-snapshots**</span> <a href="#--create-src-snapshots" title="Permalink to --create-src-snapshots" aria-label="Permalink to --create-src-snapshots" class="man-option-permalink">&#x1F517;</a>

- Take snapshots on the selected source hosts as necessary. Typically, this command should be
  called by a program (or cron job) running on each src host.

<!-- -->

<span id="--replicate" class="man-option-title">**--replicate**</span> <a href="#--replicate" title="Permalink to --replicate" aria-label="Permalink to --replicate" class="man-option-permalink">&#x1F517;</a>

- Replicate snapshots from the selected source hosts to the selected destinations hosts as
  necessary. For pull mode (recommended), this command should be called by a program (or cron job)
  running on each dst host; for push mode, on the src host; for pull-push mode on a third-party
  host, maybe with `--r2r=pull` or `--r2r=push`.

<!-- -->

<span id="--prune-src-snapshots" class="man-option-title">**--prune-src-snapshots**</span> <a href="#--prune-src-snapshots" title="Permalink to --prune-src-snapshots" aria-label="Permalink to --prune-src-snapshots" class="man-option-permalink">&#x1F517;</a>

- Prune snapshots on the selected source hosts as necessary. Typically, this command should be
  called by a program (or cron job) running on each src host.

<!-- -->

<span id="--prune-src-bookmarks" class="man-option-title">**--prune-src-bookmarks**</span> <a href="#--prune-src-bookmarks" title="Permalink to --prune-src-bookmarks" aria-label="Permalink to --prune-src-bookmarks" class="man-option-permalink">&#x1F517;</a>

- Prune bookmarks on the selected source hosts as necessary. Typically, this command should be
  called by a program (or cron job) running on each src host.

<!-- -->

<span id="--prune-dst-snapshots" class="man-option-title">**--prune-dst-snapshots**</span> <a href="#--prune-dst-snapshots" title="Permalink to --prune-dst-snapshots" aria-label="Permalink to --prune-dst-snapshots" class="man-option-permalink">&#x1F517;</a>

- Prune snapshots on the selected destination hosts as necessary. Typically, this command should
  be called by a program (or cron job) running on each dst host.

<!-- -->

<span id="--monitor-src-snapshots" class="man-option-title">**--monitor-src-snapshots**</span> <a href="#--monitor-src-snapshots" title="Permalink to --monitor-src-snapshots" aria-label="Permalink to --monitor-src-snapshots" class="man-option-permalink">&#x1F517;</a>

- Alert the user if snapshots on the selected source hosts are too old, using
  --monitor-snapshot-plan (see below). Typically, this command should be called by a program (or
  cron job) running on each src host.

<!-- -->

<span id="--monitor-dst-snapshots" class="man-option-title">**--monitor-dst-snapshots**</span> <a href="#--monitor-dst-snapshots" title="Permalink to --monitor-dst-snapshots" aria-label="Permalink to --monitor-dst-snapshots" class="man-option-permalink">&#x1F517;</a>

- Alert the user if snapshots on the selected destination hosts are too old, using
  --monitor-snapshot-plan (see below). Typically, this command should be called by a program (or
  cron job) running on each dst host.

<!-- -->

<span id="--localhost" class="man-option-title">**--localhost** *STRING*</span> <a href="#--localhost" title="Permalink to --localhost" aria-label="Permalink to --localhost" class="man-option-permalink">&#x1F517;</a>

- Hostname of localhost. Default is the hostname without the domain name, querying the Operating
  System.

<!-- -->

<span id="--src-hosts" class="man-option-title">**--src-hosts** *LIST_STRING*</span> <a href="#--src-hosts" title="Permalink to --src-hosts" aria-label="Permalink to --src-hosts" class="man-option-permalink">&#x1F517;</a>

- Hostnames of the sources to operate on. Specify a Python list literal such as `"['src1',
  'src2']"`. If omitted, reads the same list literal from stdin.

<!-- -->

<span id="--src-host" class="man-option-title">**--src-host** *STRING*</span> <a href="#--src-host" title="Permalink to --src-host" aria-label="Permalink to --src-host" class="man-option-permalink">&#x1F517;</a>

- For subsetting --src-hosts; Can be specified multiple times; Indicates to only use the
  --src-hosts that are contained in the specified --src-host values (optional).

  Example: `--src-host=src1 --src-host=src2 --src-hosts="['src1', 'src2', 'src3', 'src4']"`
  indicates to effectively only use `--src-hosts="['src1', 'src2']"`.

<!-- -->

<span id="--dst-hosts" class="man-option-title">**--dst-hosts** *DICT_STRING*</span> <a href="#--dst-hosts" title="Permalink to --dst-hosts" aria-label="Permalink to --dst-hosts" class="man-option-permalink">&#x1F517;</a>

- Dictionary that maps each destination hostname to a list of zero or more logical replication
  target names (the infix portion of snapshot name). As hostname use the real output of the
  `hostname` CLI. The target is an arbitrary user-defined name that serves as an abstraction of
  the destination hostnames for a group of snapshots, like target 'onsite', 'offsite', 'hotspare',
  a geographically independent datacenter like 'us-west', or similar. Rather than the snapshot
  name embedding (i.e. hardcoding) a list of destination hostnames where it should be sent to, the
  snapshot name embeds the user-defined target name, which is later mapped by this jobconfig to a
  list of destination hostnames.

  Example: `"{'nas': ['onsite'], 'bak-us-west': ['us-west'], 'bak-eu-west': ['eu-west'],
  'archive': ['offsite']}"`.

  With this, given a snapshot name, we can find the destination hostnames to which the snapshot
  shall be replicated. Also, given a snapshot name and its own name, a destination host can
  determine if it shall replicate the given snapshot from the source host, or if the snapshot is
  intended for another destination host, in which case it skips the snapshot. A destination host
  will receive replicas of snapshots for all targets that map to that destination host.

  Removing a mapping can be used to temporarily suspend replication to a given destination host.

<!-- -->

<span id="--dst-host" class="man-option-title">**--dst-host** *STRING*</span> <a href="#--dst-host" title="Permalink to --dst-host" aria-label="Permalink to --dst-host" class="man-option-permalink">&#x1F517;</a>

- For subsetting --dst-hosts; Can be specified multiple times; Indicates to only use the
  --dst-hosts keys that are contained in the specified --dst-host values (optional).

  Example: `--dst-host=dst1 --dst-host=dst2 --dst-hosts="{'dst1': ..., 'dst2': ..., 'dst3': ...,
  'dst4': ...}"` indicates to effectively only use `--dst-hosts="{'dst1': ..., 'dst2': ...}"`.

<!-- -->

<span id="--retain-dst-targets" class="man-option-title">**--retain-dst-targets** *DICT_STRING*</span> <a href="#--retain-dst-targets" title="Permalink to --retain-dst-targets" aria-label="Permalink to --retain-dst-targets" class="man-option-permalink">&#x1F517;</a>

- Dictionary that maps each destination hostname to a list of zero or more logical replication
  target names (the infix portion of snapshot name).

  Example: `"{'nas': ['onsite'], 'bak-us-west': ['us-west'], 'bak-eu-west': ['eu-west'],
  'archive': ['offsite']}"`. Has same format as --dst-hosts.

  As part of --prune-dst-snapshots, a destination host will delete any snapshot it has stored
  whose target has no mapping to that destination host in this dictionary. Do not remove a mapping
  here unless you are sure it's ok to delete all those snapshots on that destination host! If in
  doubt, use --dryrun mode first.

<!-- -->

<span id="--dst-root-datasets" class="man-option-title">**--dst-root-datasets** *DICT_STRING*</span> <a href="#--dst-root-datasets" title="Permalink to --dst-root-datasets" aria-label="Permalink to --dst-root-datasets" class="man-option-permalink">&#x1F517;</a>

- Dictionary that maps each destination hostname to a root dataset located on that destination
  host. The root dataset name is an (optional) prefix that will be prepended to each dataset that
  is replicated to that destination host. For backup use cases, this is the backup ZFS pool or a
  ZFS dataset path within that pool, whereas for cloning, master slave replication, or replication
  from a primary to a secondary, this can also be the empty string.

  `^SRC_HOST` and `^DST_HOST` are optional magic substitution tokens that will be auto-replaced at
  runtime with the actual hostname. This can be used to force the use of a separate destination
  root dataset per source host or per destination host.

  Example: `"{'nas': 'tank2/bak', 'bak-us-west': 'backups/bak001', 'bak-eu-west':
  'backups/bak999', 'archive': 'archives/zoo/^SRC_HOST', 'hotspare': ''}"`

<!-- -->

<span id="--src-snapshot-plan" class="man-option-title">**--src-snapshot-plan** *DICT_STRING*</span> <a href="#--src-snapshot-plan" title="Permalink to --src-snapshot-plan" aria-label="Permalink to --src-snapshot-plan" class="man-option-permalink">&#x1F517;</a>

- Retention periods for snapshots to be used if pruning src, and when creating new snapshots on
  src. Snapshots that do not match a retention period will be deleted. A zero or missing retention
  period indicates that no snapshots shall be retained (or even be created) for the given period.

  Example: `"{'prod': {'onsite': {'secondly': 40, 'minutely': 40, 'hourly': 36, 'daily': 31,
  'weekly': 12, 'monthly': 18, 'yearly': 5}, 'us-west': {'secondly': 0, 'minutely': 0, 'hourly':
  36, 'daily': 31, 'weekly': 12, 'monthly': 18, 'yearly': 5}, 'eu-west': {'secondly': 0,
  'minutely': 0, 'hourly': 36, 'daily': 31, 'weekly': 12, 'monthly': 18, 'yearly': 5}}, 'test':
  {'offsite': {'12hourly': 42, 'weekly': 12}}}"`. This example will, for the organization 'prod'
  and the intended logical target 'onsite', create and then retain secondly snapshots that were
  created less than 40 seconds ago, yet retain the latest 40 secondly snapshots regardless of
  creation time. Analog for the latest 40 minutely snapshots, 36 hourly snapshots, etc. It will
  also create and retain snapshots for the targets 'us-west' and 'eu-west' within the 'prod'
  organization. In addition, it will create and retain snapshots every 12 hours and every week for
  the 'test' organization, and name them as being intended for the 'offsite' replication target.
  The example creates snapshots with names like `prod_onsite_<timestamp>_secondly`,
  `prod_onsite_<timestamp>_minutely`, `prod_us-west_<timestamp>_hourly`,
  `prod_us-west_<timestamp>_daily`, `prod_eu-west_<timestamp>_hourly`,
  `prod_eu-west_<timestamp>_daily`, `test_offsite_<timestamp>_12hourly`,
  `test_offsite_<timestamp>_weekly`, and so on.

<!-- -->

<span id="--src-bookmark-plan" class="man-option-title">**--src-bookmark-plan** *DICT_STRING*</span> <a href="#--src-bookmark-plan" title="Permalink to --src-bookmark-plan" aria-label="Permalink to --src-bookmark-plan" class="man-option-permalink">&#x1F517;</a>

- Retention periods for bookmarks to be used if pruning src. Has same format as
  --src-snapshot-plan.

<!-- -->

<span id="--dst-snapshot-plan" class="man-option-title">**--dst-snapshot-plan** *DICT_STRING*</span> <a href="#--dst-snapshot-plan" title="Permalink to --dst-snapshot-plan" aria-label="Permalink to --dst-snapshot-plan" class="man-option-permalink">&#x1F517;</a>

- Retention periods for snapshots to be used if pruning dst. Has same format as
  --src-snapshot-plan.

<!-- -->

<span id="--monitor-snapshot-plan" class="man-option-title">**--monitor-snapshot-plan** *DICT_STRING*</span> <a href="#--monitor-snapshot-plan" title="Permalink to --monitor-snapshot-plan" aria-label="Permalink to --monitor-snapshot-plan" class="man-option-permalink">&#x1F517;</a>

- Alert the user if the ZFS 'creation' time property of the latest or oldest snapshot for any
  specified snapshot pattern within the selected datasets is too old wrt. the specified age limit.
  The purpose is to check if snapshots are successfully taken on schedule, successfully replicated
  on schedule, and successfully pruned on schedule. Process exit code is 0, 1, 2 on OK, WARNING,
  CRITICAL, respectively.

  Example DICT_STRING: `"{'prod': {'onsite': {'100millisecondly': {'warning': '650 milliseconds',
  'critical': '2 seconds'}, 'secondly': {'warning': '2 seconds', 'critical': '14 seconds'},
  'minutely': {'warning': '30 seconds', 'critical': '300 seconds'}, 'hourly': {'warning': '30
  minutes', 'critical': '300 minutes'}, 'daily': {'warning': '4 hours', 'critical': '8 hours'},
  'weekly': {'warning': '2 days', 'critical': '8 days'}, 'monthly': {'warning': '2 days',
  'critical': '8 days'}, 'yearly': {'warning': '5 days', 'critical': '14 days'}, '10minutely':
  {'warning': '0 minutes', 'critical': '0 minutes'}}, '': {'daily': {'warning': '4 hours',
  'critical': '8 hours'}}}}"`. This example alerts the user if the *latest* src or dst snapshot
  named `prod_onsite_<timestamp>_hourly` is more than 30 minutes late (i.e. more than 30+60=90
  minutes old) [warning] or more than 300 minutes late (i.e. more than 300+60=360 minutes old)
  [critical]. In addition, the example alerts the user if the *oldest* src or dst snapshot named
  `prod_onsite_<timestamp>_hourly` is more than 30 + 60x36 minutes old [warning] or more than 300
  \+ 60x36 minutes old [critical], where 36 is the number of period cycles specified in
  `src_snapshot_plan` or `dst_snapshot_plan`, respectively. Analog for the latest snapshot named
  `prod_<timestamp>_daily`, and so on.

  Note: A duration that is missing or zero (e.g. '0 minutes') indicates that no snapshots shall be
  checked for the given snapshot name pattern.

<!-- -->

<span id="--ssh-src-user" class="man-option-title">**--ssh-src-user** *STRING*</span> <a href="#--ssh-src-user" title="Permalink to --ssh-src-user" aria-label="Permalink to --ssh-src-user" class="man-option-permalink">&#x1F517;</a>

- Remote SSH username on src hosts to connect to (optional). Examples: 'root', 'alice'.

<!-- -->

<span id="--ssh-dst-user" class="man-option-title">**--ssh-dst-user** *STRING*</span> <a href="#--ssh-dst-user" title="Permalink to --ssh-dst-user" aria-label="Permalink to --ssh-dst-user" class="man-option-permalink">&#x1F517;</a>

- Remote SSH username on dst hosts to connect to (optional). Examples: 'root', 'alice'.

<!-- -->

<span id="--ssh-src-port" class="man-option-title">**--ssh-src-port** *INT*</span> <a href="#--ssh-src-port" title="Permalink to --ssh-src-port" aria-label="Permalink to --ssh-src-port" class="man-option-permalink">&#x1F517;</a>

- Remote SSH port on src host to connect to (optional).

<!-- -->

<span id="--ssh-dst-port" class="man-option-title">**--ssh-dst-port** *INT*</span> <a href="#--ssh-dst-port" title="Permalink to --ssh-dst-port" aria-label="Permalink to --ssh-dst-port" class="man-option-permalink">&#x1F517;</a>

- Remote SSH port on dst host to connect to (optional).

<!-- -->

<span id="--ssh-src-config-file" class="man-option-title">**--ssh-src-config-file** *FILE*</span> <a href="#--ssh-src-config-file" title="Permalink to --ssh-src-config-file" aria-label="Permalink to --ssh-src-config-file" class="man-option-permalink">&#x1F517;</a>

- Path to SSH ssh_config(5) file to connect to src (optional); will be passed into ssh -F CLI. The
  basename must contain the substring 'bzfs_ssh_config'.

<!-- -->

<span id="--ssh-dst-config-file" class="man-option-title">**--ssh-dst-config-file** *FILE*</span> <a href="#--ssh-dst-config-file" title="Permalink to --ssh-dst-config-file" aria-label="Permalink to --ssh-dst-config-file" class="man-option-permalink">&#x1F517;</a>

- Path to SSH ssh_config(5) file to connect to dst (optional); will be passed into ssh -F CLI. The
  basename must contain the substring 'bzfs_ssh_config'.

<!-- -->

<span id="--job-id" class="man-option-title">**--job-id** *STRING* _(required)_</span> <a href="#--job-id" title="Permalink to --job-id" aria-label="Permalink to --job-id" class="man-option-permalink">&#x1F517;</a>

- The identifier that remains constant across all runs of this particular job; will be included in
  the log file name infix. Example: mytestjob

<!-- -->

<span id="--job-run" class="man-option-title">**--job-run** *STRING*</span> <a href="#--job-run" title="Permalink to --job-run" aria-label="Permalink to --job-run" class="man-option-permalink">&#x1F517;</a>

- The identifier of this particular run of the overall job; will be included in the log file name
  suffix. Default is a hex UUID. Example: 0badc0f003a011f0a94aef02ac16083c

<!-- -->

<span id="--workers" class="man-option-title">**--workers** *INT[%]*</span> <a href="#--workers" title="Permalink to --workers" aria-label="Permalink to --workers" class="man-option-permalink">&#x1F517;</a>

- The maximum number of jobs to run in parallel at any time; can be given as a positive integer,
  optionally followed by the % percent character (min: 1, default: 100%). Percentages are relative
  to the number of CPU cores on the machine. Example: 200% uses twice as many parallel jobs as
  there are cores on the machine; 75% uses num_procs = num_cores * 0.75. Examples: 1, 4, 75%, 150%

<!-- -->

<span id="--work-period-seconds" class="man-option-title">**--work-period-seconds** *FLOAT*</span> <a href="#--work-period-seconds" title="Permalink to --work-period-seconds" aria-label="Permalink to --work-period-seconds" class="man-option-permalink">&#x1F517;</a>

- Reduces bandwidth spikes by spreading out the start of worker jobs over this much time; 0
  disables this feature (default: 0). Examples: 0, 60, 86400

<!-- -->

<span id="--jitter" class="man-option-title">**--jitter**</span> <a href="#--jitter" title="Permalink to --jitter" aria-label="Permalink to --jitter" class="man-option-permalink">&#x1F517;</a>

- Randomize job start time and host order to avoid potential thundering herd problems in large
  distributed systems (optional). Randomizing job start time is only relevant if
  --work-period-seconds > 0.

<!-- -->

<span id="--worker-timeout-seconds" class="man-option-title">**--worker-timeout-seconds** *FLOAT*</span> <a href="#--worker-timeout-seconds" title="Permalink to --worker-timeout-seconds" aria-label="Permalink to --worker-timeout-seconds" class="man-option-permalink">&#x1F517;</a>

- If this much time has passed after a worker process has started executing, kill the straggling
  worker (optional). Other workers remain unaffected. Examples: 60, 3600

<!-- -->

<span id="--repeat-if-took-more-than-seconds" class="man-option-title">**--repeat-if-took-more-than-seconds** *FLOAT*</span> <a href="#--repeat-if-took-more-than-seconds" title="Permalink to --repeat-if-took-more-than-seconds" aria-label="Permalink to --repeat-if-took-more-than-seconds" class="man-option-permalink">&#x1F517;</a>

- Repeat the entire workflow if it took longer than this much time and was successful. Use this
  (with the POSIX `timeout` CLI) before migrating VM storage to converge replication and reduce
  cutover downtime. Default is infinity, i.e. never repeat the workflow. Examples: 1, 0.1

<!-- -->

<span id="--spawn-process-per-job" class="man-option-title">**--spawn-process-per-job**</span> <a href="#--spawn-process-per-job" title="Permalink to --spawn-process-per-job" aria-label="Permalink to --spawn-process-per-job" class="man-option-permalink">&#x1F517;</a>

- Spawn a Python process per subjob instead of a Python thread per subjob (optional). The former
  is only recommended for a job operating in parallel on a large number of hosts as it helps avoid
  exceeding per-process limits such as the default max number of open file descriptors, at the
  expense of increased startup latency.

<!-- -->

<span id="--jobrunner-dryrun" class="man-option-title">**--jobrunner-dryrun**</span> <a href="#--jobrunner-dryrun" title="Permalink to --jobrunner-dryrun" aria-label="Permalink to --jobrunner-dryrun" class="man-option-permalink">&#x1F517;</a>

- Do a dry run (aka 'no-op') to print what operations would happen if the command were to be
  executed for real (optional). This option treats both the ZFS source and destination as
  read-only. Can also be used to check if the configuration options are valid.

<!-- -->

<span id="--jobrunner-log-level" class="man-option-title">**--jobrunner-log-level** *{CRITICAL,ERROR,WARN,INFO,DEBUG,TRACE}*</span> <a href="#--jobrunner-log-level" title="Permalink to --jobrunner-log-level" aria-label="Permalink to --jobrunner-log-level" class="man-option-permalink">&#x1F517;</a>

- Only emit jobrunner messages with equal or higher priority than this log level. Default is
  'INFO'.

<!-- -->

<span id="--daemon-replication-frequency" class="man-option-title">**--daemon-replication-frequency** *STRING*</span> <a href="#--daemon-replication-frequency" title="Permalink to --daemon-replication-frequency" aria-label="Permalink to --daemon-replication-frequency" class="man-option-permalink">&#x1F517;</a>

- Specifies how often the bzfs daemon shall replicate from src to dst if --daemon-lifetime is
  nonzero.

<!-- -->

<span id="--daemon-prune-src-frequency" class="man-option-title">**--daemon-prune-src-frequency** *STRING*</span> <a href="#--daemon-prune-src-frequency" title="Permalink to --daemon-prune-src-frequency" aria-label="Permalink to --daemon-prune-src-frequency" class="man-option-permalink">&#x1F517;</a>

- Specifies how often the bzfs daemon shall prune src if --daemon-lifetime is nonzero.

<!-- -->

<span id="--daemon-prune-dst-frequency" class="man-option-title">**--daemon-prune-dst-frequency** *STRING*</span> <a href="#--daemon-prune-dst-frequency" title="Permalink to --daemon-prune-dst-frequency" aria-label="Permalink to --daemon-prune-dst-frequency" class="man-option-permalink">&#x1F517;</a>

- Specifies how often the bzfs daemon shall prune dst if --daemon-lifetime is nonzero.

<!-- -->

<span id="--daemon-monitor-snapshots-frequency" class="man-option-title">**--daemon-monitor-snapshots-frequency** *STRING*</span> <a href="#--daemon-monitor-snapshots-frequency" title="Permalink to --daemon-monitor-snapshots-frequency" aria-label="Permalink to --daemon-monitor-snapshots-frequency" class="man-option-permalink">&#x1F517;</a>

- Specifies how often the bzfs daemon shall monitor snapshot age if --daemon-lifetime is nonzero.

<!-- -->

<span id="--version" class="man-option-title">**--version**</span> <a href="#--version" title="Permalink to --version" aria-label="Permalink to --version" class="man-option-permalink">&#x1F517;</a>

- Display version information and exit.

<!-- -->

<span id="--root-dataset-pairs" class="man-option-title">**--root-dataset-pairs** *SRC_DATASET DST_DATASET [SRC_DATASET DST_DATASET ...]* _(required)_</span> <a href="#--root-dataset-pairs" title="Permalink to --root-dataset-pairs" aria-label="Permalink to --root-dataset-pairs" class="man-option-permalink">&#x1F517;</a>

- Source and destination dataset pairs (excluding usernames and excluding hostnames, which will
  all be auto-appended later).
<!-- END-MANPAGE-DETAILS -->
