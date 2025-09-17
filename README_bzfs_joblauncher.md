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
# Periodic Jobs with bzfs_joblauncher
- [Introduction](#Introduction)
- [Man Page](#Man-Page)
# Introduction
<!-- DO NOT EDIT (This section was auto-generated from ArgumentParser help text as the source of "truth", via update_readme.sh) -->
<!-- BEGIN DESCRIPTION SECTION -->
This program is a convenience wrapper around [bzfs_jobrunner](README_bzfs_jobrunner.md) that
loads a YAML configuration file and launches `bzfs_jobrunner` with the corresponding arguments.

It is intended for users who prefer declaring fleet-wide job configuration in YAML instead of
writing a Python jobconfig script. The YAML schema aims for feature parity with what
`bzfs_tests/bzfs_job_example.py` facilitates. Advanced or rarely-used flags can be provided via
`extra_args` in the YAML and will be forwarded verbatim to `bzfs_jobrunner`.

Most commonly, you will run this from cron or a systemd timer on source and destination hosts,
passing one or more of the job commands below. See `README_bzfs_jobrunner.md` for operational
guidance, and `getting_started_bzfs_joblauncher.md` for an end-to-end example.

<!-- END DESCRIPTION SECTION -->


<!-- DO NOT EDIT (This section was auto-generated from ArgumentParser help text as the source of "truth", via update_readme.sh) -->
# Man Page
<!-- BEGIN HELP OVERVIEW SECTION -->
```
usage: bzfs [-h] [--recursive]
            [--include-dataset DATASET [DATASET ...]]
            [--exclude-dataset DATASET [DATASET ...]]
            [--include-dataset-regex REGEX [REGEX ...]]
            [--exclude-dataset-regex REGEX [REGEX ...]]
            [--exclude-dataset-property STRING]
            [--include-snapshot-regex REGEX [REGEX ...]]
            [--exclude-snapshot-regex REGEX [REGEX ...]]
            [--include-snapshot-times-and-ranks TIMERANGE [RANKRANGE ...]]
            [--include-snapshot-plan DICT_STRING]
            [--new-snapshot-filter-group] [--create-src-snapshots]
            [--create-src-snapshots-plan DICT_STRING]
            [--create-src-snapshots-timeformat STRFTIME_SPEC]
            [--create-src-snapshots-timezone TZ_SPEC]
            [--create-src-snapshots-even-if-not-due]
            [--zfs-send-program-opts STRING]
            [--zfs-recv-program-opts STRING]
            [--zfs-recv-program-opt STRING]
            [--preserve-properties STRING [STRING ...]]
            [--force-rollback-to-latest-snapshot]
            [--force-rollback-to-latest-common-snapshot] [--force]
            [--force-destroy-dependents] [--force-unmount]
            [--force-once] [--skip-parent]
            [--skip-missing-snapshots [{fail,dataset,continue}]]
            [--retries INT] [--retry-min-sleep-secs FLOAT]
            [--retry-max-sleep-secs FLOAT]
            [--retry-max-elapsed-secs FLOAT]
            [--skip-on-error {fail,tree,dataset}] [--skip-replication]
            [--delete-dst-datasets]
            [--delete-dst-snapshots [{snapshots,bookmarks}]]
            [--delete-dst-snapshots-no-crosscheck]
            [--delete-dst-snapshots-except]
            [--delete-dst-snapshots-except-plan DICT_STRING]
            [--delete-empty-dst-datasets [{snapshots,snapshots+bookmarks}]]
            [--monitor-snapshots DICT_STRING]
            [--monitor-snapshots-dont-warn]
            [--monitor-snapshots-dont-crit]
            [--compare-snapshot-lists [{src,dst,all,src+dst,src+all,dst+all,src+dst+all}]]
            [--cache-snapshots [{true,false}]]
            [--dryrun [{recv,send}]] [--verbose] [--quiet]
            [--no-privilege-elevation] [--no-stream]
            [--no-resume-recv]
            [--create-bookmarks {all,hourly,minutely,secondly,none}]
            [--no-use-bookmark] [--ssh-cipher STRING]
            [--ssh-src-user STRING] [--ssh-dst-user STRING]
            [--ssh-src-host STRING] [--ssh-dst-host STRING]
            [--ssh-src-port INT] [--ssh-dst-port INT]
            [--ssh-src-config-file FILE] [--ssh-dst-config-file FILE]
            [--threads INT[%]]
            [--max-concurrent-ssh-sessions-per-tcp-connection INT]
            [--bwlimit STRING]
            [--compression-program {zstd,lz4,pzstd,pigz,gzip,-}]
            [--compression-program-opts STRING]
            [--mbuffer-program {mbuffer,-}]
            [--mbuffer-program-opts STRING] [--ps-program {ps,-}]
            [--pv-program {pv,-}] [--pv-program-opts STRING]
            [--shell-program {sh,-}] [--ssh-program {ssh,hpnssh,-}]
            [--sudo-program {sudo,-}] [--zpool-program {zpool,-}]
            [--log-dir DIR] [--log-file-prefix STRING]
            [--log-file-infix STRING] [--log-file-suffix STRING]
            [--log-subdir {daily,hourly,minutely}]
            [--log-syslog-address STRING]
            [--log-syslog-socktype {UDP,TCP}]
            [--log-syslog-facility INT] [--log-syslog-prefix STRING]
            [--log-syslog-level {CRITICAL,ERROR,WARN,INFO,DEBUG,TRACE}]
            [--log-config-file STRING]
            [--log-config-var NAME:VALUE [NAME:VALUE ...]]
            [--include-envvar-regex REGEX [REGEX ...]]
            [--exclude-envvar-regex REGEX [REGEX ...]]
            [--yearly_year INT] [--yearly_month INT]
            [--yearly_monthday INT] [--yearly_hour INT]
            [--yearly_minute INT] [--yearly_second INT]
            [--monthly_month INT] [--monthly_monthday INT]
            [--monthly_hour INT] [--monthly_minute INT]
            [--monthly_second INT] [--weekly_weekday INT]
            [--weekly_hour INT] [--weekly_minute INT]
            [--weekly_second INT] [--daily_hour INT]
            [--daily_minute INT] [--daily_second INT]
            [--hourly_minute INT] [--hourly_second INT]
            [--minutely_second INT] [--secondly_millisecond INT]
            [--millisecondly_microsecond INT]
            [--zfs-recv-o-targets {full,incremental,full+incremental}]
            [--zfs-recv-o-sources STRING]
            [--zfs-recv-o-include-regex [REGEX ...]]
            [--zfs-recv-o-exclude-regex REGEX [REGEX ...]]
            [--zfs-recv-x-targets {full,incremental,full+incremental}]
            [--zfs-recv-x-sources STRING]
            [--zfs-recv-x-include-regex [REGEX ...]]
            [--zfs-recv-x-exclude-regex REGEX [REGEX ...]] [--version]
            [--help, -h]
            SRC_DATASET DST_DATASET [SRC_DATASET DST_DATASET ...]
```
<!-- END HELP OVERVIEW SECTION -->

<!-- BEGIN HELP DETAIL SECTION -->
<div id="--create-src-snapshots"></div>

**--create-src-snapshots**

*  Take snapshots on the selected source hosts as necessary. Typically executed on each src host.

<!-- -->

<div id="--replicate"></div>

**--replicate** *[]*

*  Replicate snapshots from selected source hosts to selected destination hosts. Default mode is
    'pull' if the option is given without a value.

<!-- -->

<div id="--prune-src-snapshots"></div>

**--prune-src-snapshots**

*  Prune snapshots on the selected source hosts as necessary. Typically executed on each src
    host.

<!-- -->

<div id="--prune-src-bookmarks"></div>

**--prune-src-bookmarks**

*  Prune bookmarks on the selected source hosts as necessary. Typically executed on each src
    host.

<!-- -->

<div id="--prune-dst-snapshots"></div>

**--prune-dst-snapshots**

*  Prune snapshots on the selected destination hosts as necessary. Typically executed on each dst
    host.

<!-- -->

<div id="--monitor-src-snapshots"></div>

**--monitor-src-snapshots**

*  Alert if snapshots on selected source hosts are too old, using monitor plan from the YAML.

<!-- -->

<div id="--monitor-dst-snapshots"></div>

**--monitor-dst-snapshots**

*  Alert if snapshots on selected destination hosts are too old, using monitor plan from the
    YAML.

<!-- -->

<div id="--src-host"></div>

**--src-host** *STRING*

*  For subsetting src hosts defined in YAML; can be specified multiple times.

<!-- -->

<div id="--dst-host"></div>

**--dst-host** *STRING*

*  For subsetting destination hosts defined in YAML; can be specified multiple times.

<!-- -->

<div id="--config"></div>

**--config** *FILE*

*  Path to YAML config file. See bzfs_tests/bzfs_joblauncher_example.yaml for structure and
    documentation.

<!-- -->

<div id="--version"></div>

**--version**

*  Show program's version number and exit.
