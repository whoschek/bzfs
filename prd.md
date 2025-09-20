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
 under the License.
-->

# bzfs: Product Requirements Document<a name="bzfs-product-requirements-document"></a>

| **Version** | **Date** | **Author** | **Status** | **Summary of Changes** |
| -- | -- | -- | -- | -- |
| 1.0 | 2024-10-01 | Wolfgang Hoschek | Draft | Initial version of the PRD. |
| 1.1 | 2025-04-15 | Wolfgang Hoschek | Final | Finalized Job Orchestration (bzfs_jobrunner). |
| 1.2 | 2025-06-15 | Wolfgang Hoschek | Final | Added more Use Cases. |

### Summary<a name="summary"></a>

bzfs is a command-line tool designed to solve the inherent complexities of automating ZFS snapshot replication and
snapshot management. It aims to be the `rsync` for ZFS: a single, powerful, dependency-free utility that makes robust,
high-performance data protection and availability accessible to all ZFS users. The project consists of two components:
`bzfs`, the core engine for performing snapshot operations, and `bzfs_jobrunner`, a high-level orchestrator that calls
`bzfs` as part of complex, periodic workflows to manage backup, replication, pruning and monitoring jobs across a fleet
of multiple source and destination hosts, defined in a simple, version-controllable fleet-wide configuration script.

This document outlines the requirements for `bzfs`, focusing on five key pillars: **uncompromising reliability** through
features like retries, resumable transfers and bookmark support; **maximum performance** via parallel, dependency-aware
execution and stream optimization; **extreme flexibility** through a granular filtering and configuration system;
**secure design** with support for non-root execution and comprehensive validation; and **superior ease of deployment**
with a zero-dependency model, centralized configuration and a `--dryrun` mode. The primary goal is to deliver a tool
that system administrators and DevOps engineers can trust to protect their most critical data while simplifying their
operational workload.

<!-- mdformat-toc start --slug=github --maxlevel=3 --minlevel=2 -->

- [Summary](#summary)
- [1. Introduction & Vision](#1-introduction--vision)
  - [1.1. Problem Statement](#11-problem-statement)
  - [1.2. Product Vision](#12-product-vision)
  - [1.3. Target Audience](#13-target-audience)
  - [1.4. Key Goals & Objectives](#14-key-goals--objectives)
- [2. Personas & Use Cases](#2-personas--use-cases)
  - [2.1. Persona: Dana, the DevOps Engineer](#21-persona-dana-the-devops-engineer)
  - [2.2. Persona: Alex, the System Administrator](#22-persona-alex-the-system-administrator)
  - [2.3. Persona: Sam, the Home Lab Enthusiast](#23-persona-sam-the-home-lab-enthusiast)
  - [2.4. Persona: Chris, the Storage Architect](#24-persona-chris-the-storage-architect)
- [3. Core Features & Functional Requirements](#3-core-features--functional-requirements)
  - [3.1. Core Replication Engine](#31-core-replication-engine)
  - [3.2. Snapshot Lifecycle Management](#32-snapshot-lifecycle-management)
  - [3.3. Dataset & Snapshot Selection (Filtering)](#33-dataset--snapshot-selection-filtering)
  - [3.4. Performance & Optimization](#34-performance--optimization)
  - [3.5. Security & Access Control](#35-security--access-control)
  - [3.6. Usability & Command-Line Interface](#36-usability--command-line-interface)
  - [3.7. Reliability & Error Handling](#37-reliability--error-handling)
  - [3.8. Monitoring & Validation](#38-monitoring--validation)
- [4. Job Orchestration (bzfs_jobrunner)](#4-job-orchestration-bzfs_jobrunner)
  - [4.1. Orchestration Philosophy](#41-orchestration-philosophy)
  - [4.2. Configuration Model](#42-configuration-model)
  - [4.3. Execution Model](#43-execution-model)
- [5. Non-Functional Requirements](#5-non-functional-requirements)
  - [5.1. Performance](#51-performance)
  - [5.2. Reliability](#52-reliability)
  - [5.3. Scalability](#53-scalability)
  - [5.4. Security](#54-security)
  - [5.5. Compatibility](#55-compatibility)
  - [5.6. Usability](#56-usability)
  - [5.7. Maintainability](#57-maintainability)
- [6. Assumptions & Dependencies](#6-assumptions--dependencies)
  - [6.1. System Assumptions](#61-system-assumptions)
  - [6.2. External Tool Dependencies](#62-external-tool-dependencies)
  - [6.3. User Knowledge Assumptions](#63-user-knowledge-assumptions)
- [7. Out of Scope](#7-out-of-scope)
- [8. Success Metrics & KPIs](#8-success-metrics--kpis)
- [9. Future Considerations & Roadmap](#9-future-considerations--roadmap)
- [10. Appendices](#10-appendices)
  - [10.1. Appendix A: Detailed CLI Specification (bzfs)](#101-appendix-a-detailed-cli-specification-bzfs)
  - [10.2. Appendix B: Detailed CLI Specification (bzfs_jobrunner)](#102-appendix-b-detailed-cli-specification-bzfs_jobrunner)
  - [10.3. Appendix C: Glossary of Terms](#103-appendix-c-glossary-of-terms)

<!-- mdformat-toc end -->

## 1. Introduction & Vision<a name="1-introduction--vision"></a>

This document outlines the product requirements for **`bzfs`**, a high-performance, and highly configurable command-line
utility for ZFS snapshot management and data replication. It is intended to serve as the authoritative guide for the
engineering team during the development lifecycle.

### 1.1. Problem Statement<a name="11-problem-statement"></a>

The ZFS filesystem provides powerful native capabilities for creating instantaneous, space-efficient snapshots and
replicating them incrementally. However, leveraging these features for robust, automated backup and disaster recovery
workflows at scale presents significant challenges:

- **Complexity of Automation**: While the base `zfs send` and `zfs receive` commands are powerful, building reliable,
  idempotent scripts around them is non-trivial. Scripts must correctly identify the last common snapshot, handle the
  initial full replication vs. subsequent incrementals, manage network interruptions, and resolve state conflicts (e.g.,
  a destination being modified out-of-band).
- **Performance Bottlenecks**: Serial replication of hundreds or thousands of ZFS datasets is inefficient. Achieving
  parallelism while respecting the hierarchical dependencies of ZFS datasets (parents must be replicated before
  children) requires a sophisticated scheduler. Furthermore, raw `ssh` tunnels are often sub-optimal for high-throughput
  data transfer over WAN links without proper buffering and compression.
- **Complex Retention Policies**: Implementing Grandfather-Father-Son (GFS) or other complex retention schemes requires
  complex logic to selectively create, replicate, and prune snapshots based on a combination of age, count, and naming
  patterns. Existing tools often lack the granularity to express these policies flexibly.
- **Scalability and Orchestration**: Managing replication topologies involving a fleet of multiple source and
  destination hosts (e.g., for geo-replication) leads to a combinatorial explosion of custom scripts and `cron` jobs,
  which are difficult to maintain and monitor.
- **Lack of Safety and Monitoring**: Standard ZFS tools provide limited safeguards against accidental deletion of
  critical snapshots that break the incremental replication chain. Verifying that replication is up-to-date and that
  retention policies are being met requires manual, error-prone auditing.

There is a need for a unified, dependency-free, and robust tool that abstracts this complexity into a powerful,
declarative interface, enabling users to implement sophisticated, performant, and reliable ZFS backup strategies with
ease.

### 1.2. Product Vision<a name="12-product-vision"></a>

The vision for **bzfs** is to be the de-facto open-source standard for ZFS data protection and management. It will be a
single, stand-alone, zero-dependency command-line tool that is as powerful and ubiquitous for ZFS replication as `rsync`
is for file-level synchronization. It will empower users with a declarative and expressive interface to manage the
entire lifecycle of snapshots—from atomic, high-frequency creation to intelligent, policy-based pruning—across complex,
distributed topologies, all while maximizing performance and ensuring data integrity.

### 1.3. Target Audience<a name="13-target-audience"></a>

`bzfs` is designed for technical users who manage ZFS-based storage systems. The primary audience includes:

- **System Administrators and Storage Engineers**: Professionals responsible for designing and maintaining enterprise
  backup, archival, and disaster recovery solutions. They require reliability, performance, security, and integrability
  with existing monitoring and automation frameworks.
- **DevOps and SRE Professionals**: Engineers who need to automate the replication of application data, database state,
  and container volumes. They value scriptability, declarative configuration (Infrastructure-as-Code), and
  high-frequency replication capabilities for low RPO/RTO.
- **Home Lab Enthusiasts and Power Users**: Individuals managing personal NAS devices or servers. They seek a powerful,
  easy-to-configure tool for backing up critical personal data to local disks, removable drives, or remote servers.

### 1.4. Key Goals & Objectives<a name="14-key-goals--objectives"></a>

The development of `bzfs` will be guided by the following high-level goals:

1. **Uncompromising Reliability**: The tool must be fault-tolerant and guarantee data consistency.

   - **Objective**: Implement robust error handling with configurable retry logic for transient network and system
     issues.
   - **Objective**: Utilize ZFS bookmarks to protect against accidental deletion of source snapshots, ensuring the
     incremental replication chain is preserved.
   - **Objective**: Correctly handle resumable `zfs receive` state, recovering gracefully from interrupted transfers.

2. **Maximum Performance and Efficiency**: The tool must be designed to minimize transfer times and system load.

   - **Objective**: Implement a parallel execution engine that respects ZFS dataset dependencies.
   - **Objective**: Optimize network transfers through transparent on-the-fly compression and stream buffering.
   - **Objective**: Utilize efficient SSH connection multiplexing to reduce latency for metadata-heavy operations.
   - **Objective**: Incorporate a local metadata cache to avoid unnecessary, slow `zfs list` operations for
     high-frequency jobs.

3. **Extreme Flexibility and Control**: The tool must provide users with granular control over every aspect of the
   snapshot lifecycle.

   - **Objective**: Develop a powerful filtering system for datasets and snapshots based on names, regex, time, and
     rank.
   - **Objective**: Support all common replication topologies: local, push, pull, and pull-push.
   - **Objective**: Enable the creation of complex, multi-stage workflows through a companion orchestration tool
     (`bzfs_jobrunner`).

4. **Secure Design**: The tool must operate securely, especially in multi-user and remote environments.

   - **Objective**: Support execution as a non-root user through `sudo` or ZFS's native delegation (`zfs allow`).
   - **Objective**: Prevent command injection by validating and sanitizing all user-provided inputs.
   - **Objective**: Provide mechanisms to prevent a compromised source from overwriting critical properties on the
     destination.

5. **Superior Usability**: The tool must be easy to install, configure, and debug.

   - **Objective**: Maintain zero external Python dependencies, allowing it to run as a stand-alone script.
   - **Objective**: Provide a comprehensive and clear CLI with detailed help messages.
   - **Objective**: Implement a `--dryrun` mode to allow users to safely preview operations.
   - **Objective**: Generate structured, verbose logs to facilitate easy debugging of complex workflows.

## 2. Personas & Use Cases<a name="2-personas--use-cases"></a>

### 2.1. Persona: Dana, the DevOps Engineer<a name="21-persona-dana-the-devops-engineer"></a>

- **Role**: Manages CI/CD pipelines and production infrastructure for a fast-growing tech company.
- **Goals**: Automate everything, ensure low RPO/RTO for stateful applications, integrate data management into IaC
  workflows.
- **Frustrations**: Writing and maintaining fragile shell scripts for database backups, lack of performant options for
  replicating large developer environments.

> **Use Case: High-Frequency Database Replication.** As a DevOps Engineer, I want to replicate a PostgreSQL database's
> ZFS volume from a primary server to a hot-standby server every 10 seconds, so that I can achieve a Recovery Point
> Objective (RPO) of under a minute. The system must handle network blips without manual intervention. I also need to
> prune snapshots aggressively on the primary, keeping only the last hour's worth, while retaining 6 hours of snapshots
> on the standby.
>
> **Solution with `bzfs_jobrunner`:** Dana creates a `bzfs_job_config.py` script and configures `systemd` timers to run
> periodically, either on the primary or standby host.
>
> ```
> # In bzfs_job_config.py
> root_dataset_pairs = ["data/postgres", "data/postgres"]
> target = "standby"
> src_hosts = ["primary-db"]
> dst_hosts = {"standby-db": [target]}
> retain_dst_targets = {"standby-db": [target]}
> dst_root_datasets = {"standby-db": ""}
> org = "prod"
> src_snapshot_plan = { org: {target: {"10secondly": 360, "minutely": 60}}
> dst_snapshot_plan = { org: {target: {"10secondly": 2160, "minutely": 360}}
> ```
>
> Dana configures a systemd timer that runs this command every 10 seconds:
>
> `/path/to/bzfs_job_config.py --create-src-snapshots --replicate`
>
> Dana configures a systemd timer that runs this command every 60 seconds:
>
> `/path/to/bzfs_job_config.py --monitor-src-snapshots --monitor-dst-snapshots`
>
> Dana configures a systemd timer that runs this command every 5 minutes:
>
> `/path/to/bzfs_job_config.py --prune-src-snapshots --prune-src-bookmarks --prune-dst-snapshots`

> **Use Case: Backup Integrity Check.** As a DevOps Engineer, I need to run a daily audit to verify that no snapshots
> are missing from our hot-standby server. The tool must be able to **compare the snapshot lists** of a source and
> destination recursively and produce a simple, parsable report (TSV format) of snapshots that exist only on the source,
> only on the destination, or on both.
>
> **Solution with `bzfs`:** Dana sets up a cron job.
>
> ```
> # Daily comparison job
> bzfs primary-db:data/postgres standby-db:data/postgres --recursive --compare-snapshot-lists > /var/log/bzfs/daily_diff.tsv
> ```

> **Use Case: Ephemeral Test Environment Cloning.** As a DevOps Engineer, I want to provide our developers with a script
> that can quickly clone a sanitized, multi-terabyte production database snapshot to their ZFS-based development VMs.
> The tool must be able to run in a **pull-push mode** from a central bastion host without requiring `bzfs` to be
> installed on the developer VMs. It must also exclude specific datasets containing sensitive PII data using a regex
> filter.
>
> **Solution with `bzfs`:** Developers run a script on the bastion host that executes:
>
> ```
> # Executed on bastion host
> bzfs prod-db:sanitized_clones/main_db dev-user@dev-vm:dev/main_db \
>   --recursive --no-stream \
>   --exclude-dataset-regex='.*/(users|pii_data|credentials)' \
>   --include-snapshot-regex='clone-base-2024-Q2.*'
> ```

### 2.2. Persona: Alex, the System Administrator<a name="22-persona-alex-the-system-administrator"></a>

- **Role**: Manages a large fleet of Linux and FreeBSD servers, including several petabytes of ZFS storage for user home
  directories and VMs.
- **Goals**: Ensure data is backed up reliably, meet compliance requirements for data retention, and be able to restore
  data quickly.
- **Frustrations**: Slow backup windows, complex and brittle backup scripts, difficulty auditing backup completion and
  data consistency across multiple sites.

> **Use Case: Geo-Replicated Enterprise Backup.** As a System Administrator, I need to back up all user home directories
> from our primary datacenter (N=100 source hosts) to two separate disaster recovery sites (M=2 destination hosts), one
> on-premise and one in the cloud. I need to manage this entire workflow from a single, version-controlled configuration
> script (**`bzfs_jobrunner`**). The system must create hourly, daily, and weekly snapshots on the source, but only
> replicate the daily and weekly snapshots to the DR sites. The on-premise DR site should retain snapshots for 90 days,
> while the cloud archive should retain them for 7 years. The replication must run in parallel to fit within the nightly
> backup window, and not overload any given host. I also need it to alert me via Nagios (using exit codes) if the latest
> replicated snapshot is more than 25 hours old.
>
> **Solution with `bzfs_jobrunner`:** Alex creates a `bzfs_job_config.py` script and runs it via cron on each source and
> destination host.
>
> ```
> # In bzfs_job_config.py
> root_dataset_pairs = ["tank/home", "tank/home"]
> recursive = True
> src_hosts = ["src001", "src002", ..., "src100"]
> dst_hosts = {"dr-onsite": ["onsite"], "dr-cloud": ["cloud-archive"]}
> dst_root_datasets = {"dr-onsite": "backups/^SRC_HOST", "dr-cloud": "archives/^SRC_HOST"}
> jitter = True
> monitor_snapshot_plan = {prod': {'cloud-archive': {'daily': {'warning': '25 hours', 'critical': '26 hours'}}}}
> # ... plans for replication and pruning hourly, daily, and weekly...
>
> # Cron job that runs every hour on each source host:
> 0 * * * * /path/to/bzfs_job_config.py --create-src-snapshots --prune-src-snapshots --prune-src-bookmarks
>
> # Cron job that runs at 2:01am on each destination host:
> 1 2 * * * /path/to/bzfs_job_config.py --replicate --prune-dst-snapshots
>
> # Cron job that runs every hour on each destination host:
> 0 * * * * /path/to/bzfs_job_config.py --monitor-dst-snapshots
> ```

### 2.3. Persona: Sam, the Home Lab Enthusiast<a name="23-persona-sam-the-home-lab-enthusiast"></a>

- **Role**: Runs a home server with ZFS for a media library, personal documents, and VMs.
- **Goals**: Protect irreplaceable data (family photos, documents) from disk failure or ransomware. Keep the solution
  simple and low-cost.
- **Frustrations**: Complex tools with too many dependencies, unreliable USB drive backups, lack of a simple "set it and
  forget it" solution.

> **Use Case: Backup to Removable Drive.** As a Home Lab Enthusiast, I want to back up my `tank/media` and
> `tank/documents` datasets to an external USB drive formatted with ZFS. I plug the drive in once a week. The tool
> should automatically create a new daily snapshot on the source and then **incrementally replicate** all new snapshots
> (since the last backup) to the USB drive. It should also prune snapshots older than 90 days from the USB drive, but
> always keep at least the 90 most recent ones.
>
> **Solution with `bzfs_jobrunner`:** Sam creates a `bzfs_job_config.py` script and automatically runs it once per day
> via cron.
>
> ```
> # In bzfs_job_config.py
> root_dataset_pairs = ["tank/media", "/usb/backups/media", "tank/documents", "/usb/backups/documents"]
> host = "sam"
> target = ""
> src_hosts = [host]
> dst_hosts = {host: [target]}
> retain_dst_targets = {host: [target]}
> dst_root_datasets = {host: ""}
> org = "bzfs"
> src_snapshot_plan = { org: {target: {"daily": 31, "weekly": 12}}
> dst_snapshot_plan = { org: {target: {"daily": 90, "weekly": 52}}
>
> # Cron job on each source host:
> 0 2 * * * /path/to/bzfs_job_config.py --create-src-snapshots --replicate --prune-src-snapshots --prune-dst-snapshots --monitor-dst-snapshots
> ```

### 2.4. Persona: Chris, the Storage Architect<a name="24-persona-chris-the-storage-architect"></a>

- **Role**: Designs storage solutions for a financial services company with strict performance and security
  requirements.
- **Goals**: Design highly available and performant systems, ensure compliance with data governance policies, evaluate
  and validate new technologies.
- **Frustrations**: Tools that don't respect ZFS encryption boundaries, lack of control over which ZFS properties are
  replicated, performance limitations of existing replication tools.

> **Use Case: Secure, Property-Aware Replication.** As a Storage Architect, I need to replicate an encrypted ZFS dataset
> to a DR site without exposing the encryption keys on the source server. The replication must be raw
> (`zfs send --raw`). Furthermore, I must **prevent certain ZFS properties** from the source, like `mountpoint` and
> `sharenfs`, from being applied to the destination to avoid security risks at the DR site. The tool must allow me to
> specify a blacklist of properties to preserve on the destination.
>
> **Solution with `bzfs_jobrunner`:** Chris configures the replication job with specific send options and a property
> preservation list in the `bzfs_job_config.py` script.
>
> ```
> # In bzfs_job_config.py
> extra_args += ["--zfs-send-program-opts=--raw"]
> extra_args += [
>     "--preserve-properties",
>     "mountpoint",
>     "sharenfs",
>     "encryption",
>     "keyformat",
>     "keylocation",
>     "volsize",
> ]
> ```

## 3. Core Features & Functional Requirements<a name="3-core-features--functional-requirements"></a>

This section details the functional requirements for the core `bzfs` utility, broken down by major areas of
functionality. Each requirement is assigned a unique ID for tracking purposes and includes acceptance criteria to define
"done."

### 3.1. Core Replication Engine<a name="31-core-replication-engine"></a>

The heart of `bzfs` is its ability to reliably and efficiently replicate ZFS data. This engine must be robust, flexible,
and performant, forming the foundation for all backup and synchronization tasks.

#### 3.1.1. Replication Modes<a name="311-replication-modes"></a>

| ID | Requirement | Description | Priority | Acceptance Criteria | Rationale |
| :- | :- | :- | :- | :- | :- |
| **FR-REP-001** | **Replication Topologies** | The system shall support local, push, pull, and pull-push replication modes via a unified `[[user@]host:]dataset` syntax. | Must-Have | 1. **Given** `bzfs src/data dst/data`, **Then** replication occurs locally. <br> 2. **Given** `bzfs src/data user@hostB:dst/data`, **Then** data is pushed from local to remote. <br> 3. **Given** `bzfs user@hostA:src/data dst/data`, **Then** data is pulled from remote to local. <br> 4. **Given** `bzfs user@hostA:src/data user@hostB:dst/data`, **Then** data is streamed from hostA through the initiator to hostB. | Offers maximum flexibility for different network architectures and security policies (e.g., pulling from a less-trusted zone). |
| **FR-REP-002** | **Initial Full Replication** | When no common snapshots exist between a source and destination dataset, the system shall perform a full `zfs send` of the oldest selected snapshot to establish a baseline. | Must-Have | 1. **Given** a source dataset `src/data` with snapshots `s1`, `s2`, and an empty destination dataset `dst/data`, **When** replication is run, **Then** a full send of `s1` is performed, and subsequent incrementals for `s2` follow. <br> 2. **Given** a source dataset and a destination with completely unrelated snapshots, **When** replication is run with the `--force` flag, **Then** the destination snapshots are destroyed and a full replication is initiated. | Establishes the initial synchronized state required for all future incremental updates. |
| **FR-REP-003** | **Incremental Replication** | The system shall automatically identify the most recent common snapshot (or bookmark) between source and destination and send only the incremental data since that point. | Must-Have | 1. **Given** source and destination share common snapshot `s2`, and source has new snapshots `s3` and `s4`, **When** replication is run, **Then** an incremental send from `s2` to `s4` is performed using the `-I` flag. <br> 2. **Given** the system is configured to skip intermediate snapshots with `--no-stream`, **Then** an incremental send from `s2` to `s4` is performed using the `-i` flag, not `-I`. | This is the core of efficient ZFS replication, minimizing data transfer. |

#### 3.1.2. Recovery & Safety<a name="312-recovery--safety"></a>

| ID | Requirement | Description | Priority | Acceptance Criteria | Rationale |
| :- | :- | :- | :- | :- | :- |
| **FR-REP-004** | **Resumable Transfers** | The system shall use ZFS's resumable send/receive feature (`-s` and `-t`) to recover from interrupted transfers without re-transmitting the entire stream. | Must-Have | 1. **Given** a large snapshot transfer is in progress, **When** the network connection is interrupted, **Then** a `receive_resume_token` is stored on the destination. <br> 2. **Given** a `receive_resume_token` exists, **When** replication is re-run, **Then** the `zfs send -t` command is used to resume the transfer from the point of interruption. | Increases the reliability of large transfers over unstable networks, saving time and bandwidth. |
| **FR-REP-005** | **Bookmark Utilization** | The system shall use ZFS bookmarks on the source as valid origins for incremental sends, allowing the original snapshot to be deleted from the source to save space. | Must-Have | 1. **Given** the last common object is bookmark `#s2` on the source (corresponding to snapshot `@s2` on the destination), and source has a new snapshot `@s3`, **When** replication is run, **Then** an incremental send from `#s2` to `@s3` is performed. | Decouples source and destination retention policies, enabling aggressive pruning on the source while maintaining the replication chain. |
| **FR-REP-006** | **Conflict Resolution** | The system shall detect when a destination dataset has snapshots more recent than the last common snapshot and provide configurable resolution strategies. | Must-Have | 1. **Given** a conflicting snapshot on the destination, **When** replication is run without force flags, **Then** the operation shall fail with an error. <br> 2. **Given** a conflicting snapshot on the destination, **When** replication is run with `--force-rollback-to-latest-common-snapshot`, **Then** the destination is rolled back to the last common snapshot before receiving new incrementals. | Provides a safe default (fail-fast) while allowing automated recovery for users who intend for the destination to be a mirror of the source. |
| **FR-REP-007** | **ZFS Property Handling** | The system shall allow fine-grained control over which ZFS properties are transferred during replication, supporting both send-side and receive-side filtering. | Should-Have | 1. **Given** `--zfs-send-program-opts="--props"`, **Then** all local properties are included in the send stream. <br> 2. **Given** `--zfs-recv-o-include-regex="myapp:.*"`, **Then** only user properties prefixed with `myapp:` are explicitly set on the destination via `zfs receive -o`. <br> 3. **Given** `--preserve-properties=mountpoint`, **Then** the `mountpoint` property from the send stream is ignored by the receiver. | Enables advanced use cases like replicating application-specific metadata while ignoring environment-specific settings like `mountpoint`. |

### 3.2. Snapshot Lifecycle Management<a name="32-snapshot-lifecycle-management"></a>

`bzfs` will provide comprehensive tools for automating the creation, retention, and pruning of snapshots and bookmarks.

#### 3.2.1. Snapshot Creation<a name="321-snapshot-creation"></a>

| ID | Requirement | Description | Priority | Acceptance Criteria | Rationale |
| :- | :- | :- | :- | :- | :- |
| **FR-LCM-001** | **Scheduled Snapshot Creation** | The system shall create snapshots on source datasets based on a declarative schedule specified via `--create-src-snapshots-plan`, only creating a new snapshot if it is due. | Must-Have | 1. **Given** a plan specifies `{"hourly": 24}` and the latest hourly snapshot is 59 minutes old, **When** `--create-src-snapshots` is run, **Then** no new snapshot is created. <br> 2. **Given** the same plan, but the latest hourly snapshot is 61 minutes old, **When** `--create-src-snapshots` is run, **Then** a new hourly snapshot is created. | Automates the fundamental first step of any backup strategy. The 'due-time' check makes the operation idempotent. |
| **FR-LCM-002** | **Customizable Snapshot Naming** | The system shall generate snapshot names based on a configurable format, including an organization, target, timestamp, and period, as defined in `--create-src-snapshots-plan`. | Must-Have | 1. **Given** a plan `{"prod": {"offsite": {"daily": 30}}}`, **When** a snapshot is created, **Then** its name shall match the pattern `prod_offsite_YYYY-MM-DD_HH:MM:SS_daily`. <br> 2. **Given** `--create-src-snapshots-timeformat` is set to `%Y%m%d`, **Then** the timestamp in the name shall be formatted as `YYYYMMDD`. | Structured naming is essential for automated policy-based management (filtering, pruning) and human readability. |
| **FR-LCM-003** | **Timezone and Anchor Awareness** | Snapshot creation scheduling shall be aware of timezones (`--create-src-snapshots-timezone`) and configurable time anchors (e.g., `--daily_hour=23`) to control the exact moment of creation. | Should-Have | 1. **Given** `--create-src-snapshots-timezone=America/New_York`, **When** a snapshot is created, **Then** the timestamp in the name shall reflect the current time in that timezone. <br> 2. **Given** `--daily_hour=2` and the current time is 01:59, **When** `--create-src-snapshots` is run, **Then** no daily snapshot is created (it's not due yet). | Allows for consistent snapshot timing across geographically distributed systems and aligns backups with off-peak hours. |

#### 3.2.2. Pruning and Deletion<a name="322-pruning-and-deletion"></a>

| ID | Requirement | Description | Priority | Acceptance Criteria | Rationale |
| :- | :- | :- | :- | :- | :- |
| **FR-LCM-004** | **Policy-Based Snapshot Pruning** | The system shall delete snapshots on a destination dataset based on a set of retention rules, controlled by `--delete-dst-snapshots` and the filter arguments. | Must-Have | 1. **Given** a `--delete-dst-snapshots` command with filters that select snapshots older than 30 days, **When** the command is run, **Then** only those snapshots are destroyed. <br> 2. **Given** `--delete-dst-snapshots-except` is used, **Then** the selection is inverted; snapshots *not* matching the filters are destroyed. | Automates storage space management by enforcing retention policies, preventing backups from consuming all available space. |
| **FR-LCM-005** | **Source-Aware Pruning** | When pruning a destination, the system shall, by default, only delete snapshots that do not exist on the source (by GUID). This can be overridden with `dummy` or `--delete-dst-snapshots-no-crosscheck`. | Must-Have | 1. **Given** a snapshot `@s_old` on the destination is selected for deletion by a pruning policy, but `@s_old` still exists on the source, **When** pruning is run, **Then** `@s_old` is NOT deleted. <br> 2. **Given** the same scenario, but source is set to the `dummy`, **Then** `@s_old` IS deleted. | Provides a critical safeguard. It prevents a pruning policy on the destination from deleting the last common snapshot, which would force a costly full resync. |
| **FR-LCM-006** | **Orphaned Dataset Deletion** | The system shall recursively delete datasets on the destination that no longer exist on the source (`--delete-dst-datasets`) or are empty of snapshots (`--delete-empty-dst-datasets`). | Should-Have | 1. **Given** source dataset `src/data/to_delete` is deleted, **When** `--delete-dst-datasets` is run, **Then** `dst/data/to_delete` and all its children are destroyed. <br> 2. **Given** `dst/data/empty` and all its descendants have no snapshots, **When** `--delete-empty-dst-datasets` is run, **Then** the `dst/data/empty` hierarchy is destroyed. | Keeps the destination clean and synchronized with the source's dataset structure, reclaiming space from decommissioned datasets. |

#### 3.2.3. Bookmark Management<a name="323-bookmark-management"></a>

| ID | Requirement | Description | Priority | Acceptance Criteria | Rationale |
| :- | :- | :- | :- | :- | :- |
| **FR-LCM-007** | **Bookmark Creation and Pruning** | The system shall support creating bookmarks on the source after successful replication (`--create-bookmarks`) and support pruning bookmarks on the source (`--delete-dst-snapshots=bookmarks`). | Must-Have | 1. **Given** `--create-bookmarks=all`, **When** snapshot `@s1` is replicated, **Then** a bookmark `#s1` is created on the source. <br> 2. **Given** a `--delete-dst-snapshots=bookmarks` command with a retention policy, **When** run against the source dataset, **Then** stale bookmarks are pruned. | Bookmarks are a critical component for safe, long-term replication chains. Managing their lifecycle is essential. |

### 3.3. Dataset & Snapshot Selection (Filtering)<a name="33-dataset--snapshot-selection-filtering"></a>

Highly granular filtering is key to applying policies to the correct subsets of data, enabling users to tailor backup
and replication strategies to their exact needs.

| ID | Requirement | Description | Priority | Acceptance Criteria | Rationale |
| :- | :- | :- | :- | :- | :- |
| **FR-FLT-001** | **Recursive Operation** | The system shall operate recursively on a dataset and all its descendants when the `--recursive` flag is provided. | Must-Have | **Given** the `--recursive` flag is used on `src/data`, which has a child `src/data/child`, **When** replication is run, **Then** both `src/data` and `src/data/child` are replicated. | This is the most common mode of operation for backing up entire ZFS filesystem trees. |
| **FR-FLT-002** | **Dataset Filtering by Name and Regex** | The system shall support including (`--include-dataset-regex`) and excluding (`--exclude-dataset-regex`) datasets. Excludes shall take precedence over includes. | Must-Have | **Given** `--exclude-dataset-regex=".*/temp"`, **When** operating on `src/data`, **Then** the dataset `src/data/project/temp` and its children are skipped. | Allows users to easily skip temporary, cache, or otherwise irrelevant datasets from backup and management operations. |
| **FR-FLT-003** | **Dataset Filtering by Property** | The system shall support excluding datasets based on the value of a specified ZFS user property via `--exclude-dataset-property`. | Could-Have | **Given** `--exclude-dataset-property=com.corp:no_backup` and dataset `src/data` has this property set to `true`, **When** operating recursively, **Then** `src/data` and all its descendants are skipped. | Provides an in-filesystem method for marking datasets to be ignored, which can be useful for delegating control to users. |
| **FR-FLT-004** | **Snapshot Filtering by Regex** | The system shall support including (`--include-snapshot-regex`) and excluding (`--exclude-snapshot-regex`) snapshots based on a regex match against the snapshot's name. | Must-Have | **Given** `--include-snapshot-regex=".*_daily"` and `--exclude-snapshot-regex=".*onsite.*"`, **When** replicating, **Then** `@offsite_2023_daily` is included, but `@onsite_2023_daily` and `@offsite_2023_hourly` are excluded. | Essential for implementing policies that treat different snapshot types (e.g., hourly vs. daily) differently. |
| **FR-FLT-005** | **Snapshot Filtering by Time Range** | The system shall filter snapshots based on whether their ZFS `creation` property falls within a specified time range, controlled by `--include-snapshot-times-and-ranks`. | Must-Have | 1. **Given** `--include-snapshot-times-and-ranks="2023-01-01..2023-02-01"`, **When** operating, **Then** only snapshots created in January 2023 are selected. <br> 2. **Given** `--include-snapshot-times-and-ranks="1 week ago..anytime"`, **Then** only snapshots created in the last 7 days are selected. | Enables powerful age-based retention and replication policies. |
| **FR-FLT-006** | **Snapshot Filtering by Rank** | The system shall filter snapshots based on their rank (position), controlled by `--include-snapshot-times-and-ranks`. This includes selecting the `latest N`, `oldest N`, or `all except latest N` snapshots. | Must-Have | 1. **Given** `--include-snapshot-times-and-ranks="0..0" "latest 10"`, **When** operating, **Then** only the 10 most recent snapshots are selected. <br> 2. **Given** `--include-snapshot-times-and-ranks="0..0" "all except oldest 5"`, **Then** all but the 5 oldest snapshots are selected. | Provides a robust way to ensure a minimum number of recent backups are always kept, regardless of their age. |
| **FR-FLT-007** | **Filter Combination and Grouping** | The system shall allow multiple filter criteria to be combined. Filters within a group are applied sequentially (AND logic). Separate filter groups, demarcated by `--new-snapshot-filter-group`, shall have their results combined with UNION logic. | Must-Have | 1. **Given** `--include-snapshot-regex=".*d"` and `--include-snapshot-times-and-ranks="0..0" "latest 5"`, **When** operating, **Then** only the 5 latest snapshots whose names end in 'd' are selected. <br> 2. **Given** `--include-snapshot-regex=a --new-snapshot-filter-group --include-snapshot-regex=b`, **Then** all snapshots matching 'a' OR 'b' are selected. | Enables the construction of highly specific and complex retention and replication policies to meet any business requirement. |
| **FR-FLT-008** | **Convenience Plan-Based Filtering** | The system shall provide high-level `--...-plan` options that translate a declarative retention policy into a complex chain of underlying filter groups. | Must-Have | 1. **Given** `--delete-dst-snapshots-except-plan='{"prod": {"onsite": {"daily": 30}}}'`, **When** pruning is run, **Then** the system shall generate a filter group that retains daily snapshots that are either less than 30 days old OR are one of the 30 latest daily snapshots. | Simplifies the most common use cases by abstracting away the complexity of combining multiple rank, time, and regex filters. |

### 3.4. Performance & Optimization<a name="34-performance--optimization"></a>

`bzfs` must be engineered for high performance to handle large-scale environments and meet tight backup windows.

| ID | Requirement | Description | Priority | Acceptance Criteria | Rationale |
| :- | :- | :- | :- | :- | :- |
| **FR-PERF-001** | **Parallel Dataset Processing** | The system shall process multiple independent datasets in parallel, up to a configurable number of worker threads (`--threads`). | Must-Have | **Given** `--threads=4` and 10 independent datasets to replicate, **When** the job starts, **Then** up to 4 `zfs send`/`receive` pipelines are active concurrently. | Dramatically reduces total runtime by using more CPU, network, and disk I/O resources. |
| **FR-PERF-002** | **Dependency-Aware Scheduling** | The parallel scheduler must guarantee that a parent dataset is fully processed before any of its children begin processing. | Must-Have | **Given** datasets `src/parent` and `src/parent/child`, **When** replication starts, **Then** the replication process for `src/parent/child` will not start until the process for `src/parent` has completed successfully. | Maintains the integrity of the ZFS dataset hierarchy. A child cannot be created on the destination before its parent exists. A child cannot be consistently replicated before its parent has been replicated. |
| **FR-PERF-003** | **SSH Connection Multiplexing** | The system shall use SSH's `ControlMaster` feature to multiplex multiple sessions over a single TCP connection, reducing the latency of frequent, short-lived metadata commands. | Must-Have | **Given** multiple `zfs list` commands need to be run on a remote host, **When** `bzfs` executes them, **Then** only one initial SSH authentication and TCP handshake occurs. Subsequent commands reuse the established connection. | Significantly speeds up the initial phase of replication where snapshot lists are gathered from many datasets. |
| **FR-PERF-004** | **Transparent Stream Optimization** | For remote replications, the system shall automatically and transparently insert stream compression (`zstd`) and buffering (`mbuffer`) tools into the `zfs send`/`receive` pipeline if they are available. | Should-Have | 1. **Given** `zstd` is installed on both source and destination, **When** a remote replication starts, **Then** the `zfs send` output is piped through `zstd -c`. <br> 2. **Given** `mbuffer` is not installed on the source, **Then** the replication proceeds without error and without using `mbuffer`. | Optimizes network throughput and resilience without requiring manual user configuration, adapting to the capabilities of the environment. |
| **FR-PERF-005** | **Snapshot Metadata Caching** | The system shall maintain a persistent local cache of snapshot metadata to avoid running expensive `zfs list` commands on every run, enabled via `--cache-snapshots`. | Should-Have | **Given** `--cache-snapshots` and a job runs to create hourly snapshots, **When** the job runs again 5 minutes later and the ZFS `snapshots_changed` property has not changed, **Then** `bzfs` determines no snapshot is due from the cache without running `zfs list -t snapshot`. | Drastically improves performance for daemonized or high-frequency cron jobs, where most runs will result in no action. Requires OpenZFS >= 2.2. |

### 3.5. Security & Access Control<a name="35-security--access-control"></a>

`bzfs` must be secure by design, adhering to the principle of least privilege and protecting against common
vulnerabilities.

| ID | Requirement | Description | Priority | Acceptance Criteria | Rationale |
| :- | :- | :- | :- | :- | :- |
| **FR-SEC-001** | **Non-Root Execution** | The system shall be fully functional when run by a non-root user, using either passwordless `sudo` or ZFS's native permission delegation (`zfs allow`) via `--no-privilege-elevation`. | Must-Have | 1. **Given** a non-root user with appropriate `zfs allow` permissions and the `--no-privilege-elevation` flag, **When** replication is run, **Then** all operations succeed without `sudo`. <br> 2. **Given** a non-root user with passwordless `sudo` configured for the `zfs` binary, **When** replication is run, **Then** `sudo` is prepended to privileged ZFS commands and they succeed. | Adheres to the principle of least privilege, a fundamental security best practice. |
| **FR-SEC-002** | **Command Injection Prevention** | All user-provided inputs (dataset names, snapshot names, hostnames, options) must be rigorously validated and sanitized to prevent shell command injection. | Must-Have | **Given** a dataset name of `tank/data; rm -rf /`, **When** the command is run, **Then** the program shall exit with an error indicating an invalid name and not execute the malicious command. | Prevents a critical class of security vulnerabilities. |
| **FR-SEC-003** | **Lock File for Concurrent Execution** | The system shall use a lock file to prevent multiple instances of the same job from running concurrently. | Must-Have | **Given** a `bzfs` job is already running, **When** an identical `bzfs` command is launched, **Then** the second instance shall detect the lock, print a message, and exit gracefully. | Prevents race conditions and unpredictable behavior that can arise from overlapping cron jobs. |
| **FR-SEC-004** | **Destination Property Preservation** | The system shall provide a mechanism (`--preserve-properties`) to prevent properties from a source send stream from overwriting specified properties on the destination. | Must-Have | **Given** `--preserve-properties mountpoint` is used, and the source stream contains `mountpoint=/mnt/source`, **When** the stream is received, **Then** the `mountpoint` property on the destination dataset remains unchanged. | Allows administrators to harden a destination server, preventing a (potentially compromised) source from setting dangerous properties like `mountpoint`, `canmount`, or `exec`. |
| **FR-SEC-005** | **Environment Variable Sanitization** | The system shall provide options (`--include-envvar-regex`, `--exclude-envvar-regex`) to unset potentially harmful environment variables before executing sub-processes. | Could-Have | **Given** `--exclude-envvar-regex 'LD_.*'`, **When** `bzfs` starts, **Then** all environment variables like `LD_PRELOAD` are unset before any `zfs` or `ssh` commands are executed. | Provides a more secure and hermetic execution environment, reducing the attack surface. |

### 3.6. Usability & Command-Line Interface<a name="36-usability--command-line-interface"></a>

The tool's interface must be powerful for experts but approachable for newcomers. A well-designed CLI is crucial for
adoption and effective daily use.

| ID | Requirement | Description | Priority | Acceptance Criteria | Rationale |
| :- | :- | :- | :- | :- | :- |
| **FR-USA-001** | **Zero Dependencies** | The core `bzfs` and `bzfs_jobrunner` scripts must be self-contained Python scripts with no external Python library dependencies. | Must-Have | **Given** a minimal Python 3.9+ installation, **When** the `bzfs` script is executed, **Then** it runs without any `ModuleNotFoundError`. | Simplifies deployment enormously. The tool can be copied to any server and run without needing `pip` or a virtual environment. |
| **FR-USA-002** | **Comprehensive Help and Documentation** | The CLI shall provide detailed `--help` output with clear explanations and examples for every option. | Must-Have | **When** `bzfs --help` is run, **Then** the output is well-formatted, comprehensive, and includes practical examples. | Makes the tool's powerful features discoverable and reduces the learning curve. |
| **FR-USA-003** | **Dry Run Mode** | A `--dryrun` mode shall be available that simulates all operations and prints the commands that would be executed, without making any changes to the source or destination. | Must-Have | 1. **Given** a replication command with `--dryrun`, **When** it is run, **Then** the log output shows the `zfs send` and `zfs receive` commands that would be executed. <br> 2. **Given** `--dryrun=recv`, **Then** the send stream is generated and sent, but discarded by `zfs receive -n`. | Allows users to safely test and validate complex filter and pruning rules before applying them destructively. |
| **FR-USA-004** | **Verbose Logging** | The system shall provide multiple levels of verbosity (`-v`, `-v -v`, etc.) to aid in debugging. At the highest level, it shall print the exact shell commands being executed. | Must-Have | **Given** `-v -v` is used, **When** replication is run, **Then** the log contains the full, copy-pasteable shell command for the SSH and ZFS pipeline. | Essential for diagnosing complex issues with remote execution, permissions, or pipeline tools. |
| **FR-USA-005** | **Argument-from-File Support** | The system shall support reading lists of arguments from a file using a `+filename` syntax for options like `root_dataset_pairs` and regex filters. | Should-Have | **Given** a file `datasets.txt` containing `src1\tdst1\nsrc2\tdst2`, **When** `bzfs +datasets.txt` is run, **Then** the system processes both dataset pairs. | Simplifies the management of very large lists of datasets or filters, which can be version-controlled separately. |
| **FR-USA-006** | **Daemon Mode** | The system shall support a long-running daemon mode for high-frequency operations, triggered by `--daemon-lifetime`. | Should-Have | **Given** `--daemon-lifetime=60s` and `--daemon-frequency=secondly`, **When** the command is run, **Then** the process enters a loop, performing its task approximately every second and exiting after 60 seconds. | Reduces the overhead of process startup and environment detection for tasks that need to run very frequently (e.g., every few seconds). |

### 3.7. Reliability & Error Handling<a name="37-reliability--error-handling"></a>

`bzfs` must be resilient to common failures in distributed systems.

| ID | Requirement | Description | Priority | Acceptance Criteria | Rationale |
| :- | :- | :- | :- | :- | :- |
| **FR-REL-001** | **Configurable Retry Mechanism** | The system shall retry failed operations that are deemed transient using a configurable exponential backoff with jitter, controlled by `--retries` and `--retry-*` options. | Must-Have | **Given** a `zfs receive` command fails due to a temporary network hiccup, **When** `--retries=3` is set, **Then** the operation is attempted up to 3 more times with increasing, randomized delays. | Improves the overall success rate of jobs running over unreliable networks without requiring manual intervention. |
| **FR-REL-002** | **Configurable Behavior on Fatal Error** | When a non-retryable error occurs for a dataset, the system's behavior shall be configurable via `--skip-on-error`: fail immediately, skip the failed dataset, or skip the entire subtree. | Must-Have | 1. **Given** `--skip-on-error=fail` and an unrecoverable error occurs, **Then** the entire `bzfs` process exits with a non-zero status. <br> 2. **Given** `--skip-on-error=tree`, **Then** the error is logged, the failed dataset and its children are skipped, and processing continues with the next sibling dataset. | Allows users to choose between a fail-fast approach (prioritizing consistency) and a best-effort approach (prioritizing availability). |
| **FR-REL-003** | **Graceful Signal Handling** | The system shall trap termination signals (SIGINT, SIGTERM) to perform graceful shutdown, including terminating child processes and cleaning up SSH control sockets. | Must-Have | **Given** a `bzfs` process with multiple active sub-processes is running, **When** it receives a SIGTERM signal, **Then** it shall send SIGTERM to all its descendant processes before exiting. | Prevents orphaned processes and stale resource files (like SSH sockets or lock files), ensuring a clean system state. |

### 3.8. Monitoring & Validation<a name="38-monitoring--validation"></a>

`bzfs` will provide features to audit and monitor the state of replication.

| ID | Requirement | Description | Priority | Acceptance Criteria | Rationale |
| :- | :- | :- | :- | :- | :- |
| **FR-MON-001** | **Snapshot List Comparison** | The system shall provide a `--compare-snapshot-lists` feature that efficiently compares the snapshots on a source and destination and produces a detailed report. | Must-Have | 1. **Given** the command is run, **Then** a TSV file is generated listing snapshots unique to the source, unique to the destination, and common to both. <br> 2. **Given** the command is run, **Then** summary statistics are logged to the console, including the number of missing snapshots and the age of the latest common snapshot. | Provides a powerful auditing tool to verify that backups are consistent and up-to-date, without having to manually parse `zfs list` output. |
| **FR-MON-002** | **Snapshot Age Monitoring** | The system shall provide a `--monitor-snapshots` feature to check if the latest (or oldest) snapshot matching a pattern is older than a configurable threshold. | Must-Have | 1. **Given** a monitoring plan with a `critical` threshold of `26 hours` for daily snapshots, **When** the latest daily snapshot is more than 26 hours old, **Then** the program shall exit with a CRITICAL status code (2). <br> 2. **Given** the same plan with a `warning` threshold of `25 hours`, **When** the latest snapshot is more than 25 hours old, **Then** the program shall exit with a WARNING status code (1). | Enables integration with standard monitoring systems (like Nagios, Prometheus, etc.) to proactively alert administrators when backup jobs are failing or stalled. |
| **FR-MON-003** | **Progress Reporting** | During long-running data transfers, the system shall display a live, updating progress bar in the console (if `pv` is available and the output is a TTY), showing transfer speed, data transferred, and ETA. | Must-Have | **Given** a large snapshot is being replicated and `pv` is installed, **When** viewing the console, **Then** a single status line updates periodically with transfer statistics. | Provides essential real-time feedback to the user on the progress and performance of long-running operations. |

## 4. Job Orchestration (bzfs_jobrunner)<a name="4-job-orchestration-bzfs_jobrunner"></a>

To address the complexity of managing workflows across many hosts, a companion orchestration tool, `bzfs_jobrunner`,
will be developed.

### 4.1. Orchestration Philosophy<a name="41-orchestration-philosophy"></a>

`bzfs_jobrunner` is a stateless orchestrator, not a monolithic server. It reads a configuration defining the entire
fleet of hosts and their replication relationships, and based on the command-line arguments (e.g., `--replicate`,
`--create-src-snapshots`) and host context (e.g., `--src-host=hostA`), it generates and executes the necessary `bzfs`
commands. This promotes an **"Infrastructure as Code"** approach where the entire backup strategy is defined in a
single, version-controllable file.

### 4.2. Configuration Model<a name="42-configuration-model"></a>

The configuration is a Python script, offering maximum flexibility. All configuration is centralized in this single file
for an "Infrastructure as Code" approach.

| ID | Requirement | Description | Priority | Acceptance Criteria | Rationale |
| :- | :- | :- | :- | :- | :- |
| **FR-ORC-001** | **Python-Based Job Configuration** | The orchestrator shall be driven by a user-provided Python script that defines hosts, datasets, and plans, and then invokes the `bzfs_jobrunner` main function. | Must-Have | **Given** an example job script is executed, **Then** it shall construct the correct `bzfs_jobrunner` command-line arguments from its Python variables and execute the job. | Using Python for configuration avoids creating a new configuration language and provides users with ultimate flexibility (loops, conditionals, variables, etc.) to define their environment. |
| **FR-ORC-002** | **Multi-Host and Target Definition** | The configuration shall allow defining N source hosts and M destination hosts via `--src-hosts` and `--dst-hosts`. Destination hosts shall be mapped to logical "targets" that correspond to the target infix in snapshot names. | Must-Have | **Given** `dst_hosts = {"hostA": ["onsite"], "hostB": ["offsite"]}`, **When** replicating from a source, **Then** `onsite_...` snapshots are sent to hostA and `offsite_...` snapshots are sent to hostB. | This mapping is the core mechanism that links snapshot retention policies to physical destinations. |
| **FR-ORC-003** | **Dynamic Destination Paths** | The configuration for destination root datasets (`--dst-root-datasets`) shall support magic substitution tokens like `^SRC_HOST` and `^DST_HOST`. | Must-Have | **Given** `dst_root_datasets = {"hostB": "backups/^SRC_HOST"}` and replicating from source `hostA`, **When** the job runs, **Then** the effective destination root on hostB is `backups/hostA`. | Crucial for N-to-M replication, as it provides a simple way to namespace backups from different sources on a shared destination host, preventing collisions. |

### 4.3. Execution Model<a name="43-execution-model"></a>

| ID | Requirement | Description | Priority | Acceptance Criteria | Rationale |
| :- | :- | :- | :- | :- | :- |
| **FR-ORC-004** | **Parallel Sub-Job Execution** | `bzfs_jobrunner` shall execute the generated `bzfs` commands (sub-jobs) in parallel, up to a configurable worker limit (`--workers`). | Must-Have | **Given** a job that replicates from 1 source to 5 destinations and `--workers=3`, **When** the job runs, **Then** up to 3 `bzfs` processes are running concurrently. | Maximizes throughput by parallelizing work across different source-destination pairs. |
| **FR-ORC-005** | **Task Spreading and Jitter** | The orchestrator shall support spreading the start times of sub-jobs over a configurable period (`--work-period-seconds`) and optionally randomizing their execution order (`--jitter`). | Should-Have | 1. **Given** `--work-period-seconds=60` and 60 jobs, **When** the orchestrator runs, **Then** a new sub-job is started approximately every second. <br> 2. **Given** `--jitter`, **Then** the order in which source/destination hosts are processed is randomized. | Prevents the "thundering herd" problem in large-scale deployments, where hundreds of hosts might start their backup jobs at the exact same time, overwhelming network or storage resources. |
| **FR-ORC-006** | **Dependency-Aware Sub-Job Scheduling** | The orchestrator's parallel scheduler shall support defining dependencies between sub-jobs using a hierarchical naming scheme and a special barrier marker (`~`). | Must-Have | **Given** sub-jobs named `task/repl/hostA` and `task/repl/hostB`, and another job `task/~/prune`, **When** the orchestrator runs, **Then** the `prune` job will not start until replication to both `hostA` and `hostB` jobs have completed successfully. | Enables the creation of complex, multi-phase workflows, such as ensuring all replications are complete before a global pruning or verification step begins. |

## 5. Non-Functional Requirements<a name="5-non-functional-requirements"></a>

### 5.1. Performance<a name="51-performance"></a>

- **NFR-PERF-001:** For metadata-only operations (e.g., `--create-src-snapshots` with caching enabled, no snapshots
  due), the tool must complete its check on a dataset with thousands of snapshots in under **20ms** on average.
- **NFR-PERF-002:** When replicating over a 10Gbps network link with low latency, the tool's overhead (including SSH,
  compression, and buffering) should not reduce throughput by more than 15% compared to a raw
  `zfs send | nc | zfs   receive` pipeline.
- **NFR-PERF-003:** The parallel scheduler must scale efficiently, handling a queue of 1,000,000+ datasets with minimal
  CPU overhead in the main coordination thread.

### 5.2. Reliability<a name="52-reliability"></a>

- **NFR-REL-001:** The tool must behave correctly and not corrupt data or state when faced with abrupt termination
  (e.g., SIGKILL, power loss). Subsequent runs must be able to recover gracefully.
- **NFR-REL-002:** The mean time between failures (MTBF) for the core replication logic under normal operating
  conditions shall be extremely high. The code must be covered by an extensive suite of unit and integration tests.

### 5.3. Scalability<a name="53-scalability"></a>

- **NFR-SCA-001:** The tool must efficiently handle datasets containing over 100,000 snapshots.
- **NFR-SCA-002:** The `bzfs_jobrunner` must be able to orchestrate jobs across thousands of source and destination
  hosts.

### 5.4. Security<a name="54-security"></a>

- **NFR-SEC-001:** The codebase must pass static analysis scans (e.g., Bandit) with zero high-severity vulnerabilities.
- **NFR-SEC-002:** The tool must not write any sensitive information (passwords, keys) to log files.

### 5.5. Compatibility<a name="55-compatibility"></a>

- **NFR-COM-001:** The tool must be compatible with Python 3.9 and all subsequent versions.
- **NFR-COM-002:** The tool must be tested and verified to work with major versions of OpenZFS (>= 2.1) on Linux and
  FreeBSD.

### 5.6. Usability<a name="56-usability"></a>

- **NFR-USA-001:** Command-line options shall be logically grouped and named to be as intuitive as possible.
- **NFR-USA-002:** Error messages must be clear, concise, and actionable, guiding the user toward a solution.

### 5.7. Maintainability<a name="57-maintainability"></a>

- **NFR-MNT-001:** The codebase must adhere to PEP 8 style guidelines and be enforced by automated formatters (Black)
  and linters (Ruff).
- **NFR-MNT-002:** The code shall be well-documented with docstrings and inline comments where necessary.
- **NFR-MNT-003:** Test coverage shall be maintained above 90%.

## 6. Assumptions & Dependencies<a name="6-assumptions--dependencies"></a>

### 6.1. System Assumptions<a name="61-system-assumptions"></a>

- The source and destination systems are Unix-like operating systems (Linux, FreeBSD) with ZFS installed.
- Network connectivity and proper firewall rules exist between the initiator, source, and destination hosts for remote
  operations.
- SSH key-based authentication is configured for passwordless access for remote operations.

### 6.2. External Tool Dependencies<a name="62-external-tool-dependencies"></a>

`bzfs` itself has no Python dependencies, but relies on the presence of several standard command-line tools for its full
functionality. The system will gracefully degrade if optional tools are missing.

- **Required on Source/Destination:** `zfs`.
- **Required on Initiator (for remote ops):** `ssh`, `sh`.
- **Optional (for performance/usability):** `sh`, `zpool`, `zstd` (or other compressor), `mbuffer`, `pv`, `ps`.

### 6.3. User Knowledge Assumptions<a name="63-user-knowledge-assumptions"></a>

- Users are expected to have a working knowledge of ZFS concepts (datasets, snapshots, properties).
- Users are expected to be comfortable with the command line and basic shell scripting.

## 7. Out of Scope<a name="7-out-of-scope"></a>

The following items are explicitly out of scope for version 1.0:

- **Graphical User Interface (GUI):** `bzfs` is a command-line tool only.
- **Centralized Management Server:** `bzfs_jobrunner` is a stateless orchestrator, not a persistent server with a job
  database or API.
- **ZFS Pool and Filesystem Provisioning:** The tool assumes ZFS pools already exist. It can create datasets, but not
  pools.
- **Native Cloud Object Storage Integration:** The tool replicates to ZFS filesystems. While these can reside on cloud
  VMs, direct replication to/from S3, GCS, etc., is not supported.
- **Non-ZFS Filesystems:** The tool is exclusively for ZFS.
- **Windows Support:** While the code is Python, the reliance on Unix-like shell conventions and tools makes native
  Windows support out of scope. (WSL is a viable platform).

## 8. Success Metrics & KPIs<a name="8-success-metrics--kpis"></a>

The success of the project will be measured by:

- **Reliability:** Zero P0 bug reports (i.e., data corruption or data loss) within the first 6 months of a stable
  release.
- **Performance:** Achieve at least 85% of raw `netcat` throughput on a 10GbE link for large stream replication, as
  measured by internal benchmarks.
- **Adoption:** Achieve 100+ GitHub stars and 5,000+ downloads from PyPI within the first year of a stable release.
- **Community Health:** Median time to first response on new issues is under 48 hours. Median time to close critical bug
  reports is under 7 days.

## 9. Future Considerations & Roadmap<a name="9-future-considerations--roadmap"></a>

While out of scope for v1.0, the following features may be considered for future releases:

- **Cloud Integration:** Native support for sending/receiving ZFS streams to/from object storage (S3, etc.).
- **Plugin Architecture:** Allow users to write custom hooks that run before/after snapshotting or replication (e.g., to
  quiesce a database).
- **Web Dashboard:** A simple web interface for monitoring the status/metrics of `bzfs_jobrunner` jobs.

## 10. Appendices<a name="10-appendices"></a>

### 10.1. Appendix A: Detailed CLI Specification (bzfs)<a name="101-appendix-a-detailed-cli-specification-bzfs"></a>

This section provides a structural example of the exhaustive specification for every command-line argument for the core
`bzfs` utility.

- `root_dataset_pairs SRC_DATASET DST_DATASET [...]`

  - **Description**: Positional argument defining one or more source and destination dataset pairs. This is the primary
    input to the program.
  - **Format**: Each pair consists of two strings: the source and the destination. The format for each dataset string is
    `[[user@]host:]pool/dataset`.
  - **Special Syntax**: If an argument begins with `+`, it is interpreted as a path to a file. The file must contain
    tab-separated source and destination pairs, one pair per line.

- `--recursive`, `-r`

  - **Description**: A flag that enables recursive operation on all descendant datasets of the specified
    source/destination roots.

- `--include-dataset DATASET`

  - **Description**: Specifies a dataset to include in the operation. Can be specified multiple times. Acts as a
    shorthand for generating an `--include-dataset-regex`.

- `--exclude-dataset DATASET`

  - **Description**: Specifies a dataset to exclude. Exclusion rules override inclusion rules.

- `--include-dataset-regex REGEX`

  - **Description**: Includes datasets whose relative path matches the given regex.
  - **Special Syntax**: A leading `!` negates the match.

- `--exclude-dataset-regex REGEX`

  - **Description**: Excludes datasets matching the regex. Default will be set to a sensible pattern to exclude common
    temporary datasets (e.g., `.*/temp.*`).

*... (additional arguments to be documented here in full detail)*

### 10.2. Appendix B: Detailed CLI Specification (bzfs_jobrunner)<a name="102-appendix-b-detailed-cli-specification-bzfs_jobrunner"></a>

This section provides a structural example of the exhaustive specification for every command-line argument for the
`bzfs_jobrunner` orchestration utility.

*... (arguments to be documented here in full detail)*

### 10.3. Appendix C: Glossary of Terms<a name="103-appendix-c-glossary-of-terms"></a>

- **Bookmark**: A lightweight ZFS object that marks a point in a dataset's history, allowing the original snapshot to be
  deleted while still serving as a valid source for an incremental send.
- **Common Snapshot/Bookmark**: The most recent snapshot or bookmark, identified by its unique GUID, that exists on both
  the source and destination datasets. It serves as the starting point for an incremental replication.
- **Daemon Mode**: A long-running process mode for `bzfs` that repeatedly performs its configured task at a high
  frequency, designed to minimize process startup overhead.
- **Dry Run**: A simulation mode where the tool logs the actions it would take without actually modifying any data on
  the destination.
- **Filter Group**: A set of snapshot filter options that are applied sequentially. The results of multiple groups can
  be combined with UNION logic.
- **GUID (Globally Unique Identifier)**: A unique identifier assigned by ZFS to every snapshot. Snapshots with the same
  GUID contain identical data.
- **Pull Mode**: Replication is initiated from the destination host, which 'pulls' data from the source.
- **Push Mode**: Replication is initiated from the source host, which 'pushes' data to the destination.
- **Pull-Push Mode**: Replication is initiated from a third-party host that pulls from the source and pushes to the
  destination.
- **Rank**: The ordinal position of a snapshot when all snapshots in a dataset are sorted by creation time.
- **Target**: In `bzfs_jobrunner`, a logical name (e.g., `onsite`, `cloud_archive`) used in snapshot names to direct
  replication to specific destination hosts.
