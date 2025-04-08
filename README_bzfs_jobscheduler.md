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

# Geo-Replicated Backup with bzfs_jobscheduler

- [Introduction](#Introduction)
- [Man Page](#Man-Page)

# Introduction

<!-- DO NOT EDIT (auto-generated from ArgumentParser help text as the source of "truth", via update_readme.py) -->
<!-- BEGIN DESCRIPTION SECTION -->
WARNING: For now, `bzfs_jobscheduler` is work-in-progress, and as such may still change in
incompatible ways.

This program simplifies the deployment of an efficient geo-replicated backup service where each of
the N destination hosts is located in a separate geographic region and pulls replicas from (the
same set of) M source hosts, using the same shared [multisrc
jobconfig](bzfs_tests/bzfs_job_example_multisrc.py) script. The number of source hosts can be
large. N=2 or N=3 are typical geo-replication factors.

This scheduler program is a light-weight convenience wrapper around
[bzfs_jobrunner](README_bzfs_jobrunner.md) that simplifies the reliable and efficient scheduling
of a variable number of independent worker jobs. A failure of a worker job does not affect the
operation of the other worker jobs. The scheduler is called by a [multisrc
jobconfig](bzfs_tests/bzfs_job_example_multisrc.py) script.

`stdin` must contain a list of zero or more CLI commands, where each command is a list of one or
more strings.

This scheduler program submits each command as a job to `bzfs_jobrunner`, which in turn
delegates most of the actual work to the `bzfs` CLI. Uses an "Infrastructure as Code"
approach.

<!-- END DESCRIPTION SECTION -->


<!-- FINE TO EDIT -->

# Man Page

<!-- DO NOT EDIT (auto-generated from ArgumentParser help text as the source of "truth", via update_readme.py) -->
<!-- BEGIN HELP OVERVIEW SECTION -->
```
usage: bzfs_jobscheduler [-h] [--workers INT[%]]
                         [--work-period-seconds FLOAT]
                         [--worker-timeout-seconds FLOAT]
```
<!-- END HELP OVERVIEW SECTION -->

<!-- BEGIN HELP DETAIL SECTION -->
<div id="--workers"></div>

**--workers** *INT[%]*

*  The maximum number of jobs to run in parallel at any time; can be given as a positive integer,
    optionally followed by the % percent character (min: 1, default: 100%). Percentages are
    relative to the number of CPU cores on the machine. Example: 200% uses twice as many parallel
    jobs as there are cores on the machine; 75% uses num_procs = num_cores * 0.75. Examples: 1,
    4, 75%, 150%

<!-- -->

<div id="--work-period-seconds"></div>

**--work-period-seconds** *FLOAT*

*  Reduces bandwidth spikes by evenly spreading the start of worker jobs over this much time; 0
    disables this feature (default: 0). Examples: 0, 60, 86400

<!-- -->

<div id="--worker-timeout-seconds"></div>

**--worker-timeout-seconds** *FLOAT*

*  If this much time has passed after a worker process has started executing, kill the straggling
    worker (optional). Other workers remain unaffected. Examples: 60, 3600
