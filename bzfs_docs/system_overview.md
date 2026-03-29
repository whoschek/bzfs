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

# Why bzfs exists

`bzfs` exists because ZFS gives you powerful primitives for durability and replication, but it does not give you an
operationally complete replication system out of the box.

At small scale, a human can remember which snapshots to create, which ones are safe to prune, which destination is
behind, which host should pull or push, and which interrupted transfer can be resumed safely. At fleet scale, that turns
into a reliability problem. The hard part is not calling `zfs send` once. The hard part is making snapshot creation,
replication, pruning, monitoring, retries, and recovery behave predictably across many datasets and hosts, every day,
under partial failure.

This repository exists to close that gap:

- It turns raw ZFS primitives into a repeatable disaster-recovery and high-availability workflow.
- It keeps the system safe under interruption, retries, divergence, and operator mistakes.
- It stays lightweight enough to run in barebones server environments with no required Python dependencies.
- It preserves direct access to native ZFS semantics instead of hiding them behind a heavy control plane.

The guiding idea is simple: prefer explicit, inspectable automation over clever hidden state.

# What Problem the System Solves

The system manages the full lifecycle of replicated ZFS snapshots:

- create source snapshots on a schedule,
- replicate them incrementally to one or more destinations,
- prune snapshots and bookmarks according to retention policy,
- monitor whether expected snapshots are appearing on time,
- recover cleanly from interrupted transfers.

That makes the system useful for:

- backup,
- disaster recovery,
- hot or warm standbys,
- fan-out replication to multiple regions,
- high-frequency low-latency replication for selected datasets.

Without this layer, operators usually end up with brittle shell scripts, one-off cron jobs, unclear retention rules, and
recovery procedures that are only understood by the person who wrote them.

# System in One Sentence

`bzfs` is the data-plane engine that performs safe ZFS snapshot operations, and `bzfs_jobrunner` is the control-plane
orchestrator that decides which hosts and dataset pairs should run those operations on a schedule.

# The Core Design Choice

This project deliberately splits the system into two layers:

## `bzfs`: the execution engine

`bzfs` is responsible for the mechanics:

- listing datasets and snapshots,
- filtering what is in scope,
- deciding whether replication should be full or incremental,
- choosing efficient `zfs send -i` or `zfs send -I` steps,
- invoking `zfs send`, `zfs receive`, `ssh`, and optional helper tools,
- handling retries, progress reporting, rollback checks, resumable receive state, and deletion.

It is designed to be usable directly as a standalone CLI.

## `bzfs_jobrunner`: the fleet orchestrator

`bzfs_jobrunner` is responsible for policy execution across many hosts:

- selecting source hosts and destination hosts,
- mapping logical replication targets to real hosts,
- expanding a shared job configuration into concrete `bzfs` invocations,
- coordinating periodic workflows such as create, replicate, prune, and monitor.

It does not replace `bzfs`. It repeatedly composes and launches it.

## Why the split matters

This split keeps the system easier to reason about:

- the replication engine stays close to ZFS semantics,
- the orchestration layer can evolve without entangling core transfer logic,
- a user can adopt only `bzfs`, or can adopt the full fleet workflow with `bzfs_jobrunner`,
- failures are easier to localize because policy and execution are separate concerns.

# The Mental Model

A new engineer should hold this picture in mind:

1. A source dataset tree contains snapshots.
2. Snapshot names encode policy-relevant information such as organization, target, timestamp, and period.
3. `bzfs` compares source and destination state to find the latest common snapshot or bookmark.
4. If no common basis exists, it performs a full send of the oldest selected source snapshot.
5. If a common basis exists, it performs one or more incremental sends until the destination catches up.
6. Separate flows can create new snapshots, prune old snapshots, prune bookmarks, compare trees, or monitor staleness.
7. `bzfs_jobrunner` runs these flows repeatedly across many hosts using one shared Python job config.

The system is therefore not "a backup script." It is a state convergence engine for snapshot trees.

# The Important Domain Concepts

## Dataset tree

A dataset may contain descendant datasets. Many operations are recursive, so the real unit of work is often a dataset
tree, not a single dataset.

## Snapshot identity

Snapshot names are useful, but GUIDs matter for correctness. The system uses GUID-aware logic when it needs to know
whether two snapshots are truly the same snapshot across source and destination.

## Latest common snapshot

Incremental replication depends on a shared basis. The most recent common snapshot or bookmark is the anchor from which
further sends can proceed safely.

## Bookmarks

Bookmarks are lighter-weight than snapshots and can still serve as an incremental base. They improve safety and space
efficiency in retention workflows.

## Targets

Snapshot names can encode logical targets such as `onsite`, `offsite`, or `us-west`. A job configuration maps those
logical targets to real destination hosts. That indirection is what lets one snapshot policy fan out to multiple
deployments cleanly.

## Retention plans

Creation, pruning, and monitoring are policy-driven. The job config defines how many `minutely`, `hourly`, `daily`, and
other classes of snapshots should exist on source, destination, or as bookmarks.

# The Safety Model

The system is built around a few invariants.

## Source is normally treated as read-only

Unless explicitly asked to create or prune snapshots or bookmarks, `bzfs` treats the source as something to read from,
not something to mutate.

## Destination is normally append-only

Normal replication adds newer snapshots. Destructive actions on the destination are explicit options, not implicit
cleanup.

## Recovery must be idempotent

A partially failed run should not force manual repair in the common case. The next run should be able to retry, resume,
or conservatively fall back without corrupting the replication flow.

## Native ZFS semantics stay visible

This system does not invent its own replication format. It uses `zfs send` and `zfs receive`, plus bookmarks,
properties, and resumable receive support. That keeps behavior close to the platform underneath it.

## Dry runs must be inspectable

The project logs the commands it would execute so an operator can understand and replay them manually.

# How a Replication Run Works

At a high level, one replication task follows this sequence:

1. Determine the source dataset and the corresponding destination dataset.
2. List source snapshots and bookmarks, and destination snapshots.
3. Filter both sides according to dataset and snapshot policy.
4. Find the latest common snapshot or bookmark.
5. If required, check whether destination rollback or reconciliation is necessary.
6. If there is no common basis, perform a full replication step.
7. If there is a common basis, compute the smallest efficient sequence of incremental send steps.
8. Execute `zfs send` and `zfs receive`, optionally across SSH with buffering, compression, progress reporting, and
   resumable receive support.
9. Update caches and logs so later runs can avoid unnecessary work.

The implementation of those mechanics lives primarily in:

- [bzfs_main/bzfs.py](/Users/hoschek/unix/cloud/repos/bzfs/bzfs_main/bzfs.py)
- [bzfs_main/replication.py](/Users/hoschek/unix/cloud/repos/bzfs/bzfs_main/replication.py)
- [bzfs_main/incremental_send_steps.py](/Users/hoschek/unix/cloud/repos/bzfs/bzfs_main/incremental_send_steps.py)

# Why Performance Looks the Way It Does

The project is optimized for environments where command startup cost, SSH latency, and repeated `zfs list` calls become
real bottlenecks.

Several design choices follow from that:

- dataset and snapshot processing is parallelized,
- command arguments are batched to stay under operating-system limits,
- progress is aggregated across multiple concurrent transfers,
- snapshot metadata can be cached using ZFS `snapshots_changed` plus small local inode-backed cache files,
- optional helper tools such as `pv`, `zstd`, and `mbuffer` are used when available but are never required.

The cache design is intentionally conservative: cache hits are an optimization, not a source of truth. If trust in the
cache is not justified, the system falls back to live ZFS queries.

# Why the Job Configuration Is Python

The shared job configuration is a Python script rather than a custom DSL or static YAML schema because the problem is
inherently conditional:

- hosts vary,
- targets vary,
- destination roots vary,
- retention policies vary,
- the same fleet-wide policy often needs small computed adjustments.

Using Python keeps the configuration expressive while staying easy to version-control, diff, and review. The example
entry point is
[bzfs_testbed/bzfs_job_testbed.py](/Users/hoschek/unix/cloud/repos/bzfs/bzfs_testbed/bzfs_job_testbed.py).

# Where to Start Reading the Code

If you want to understand the system quickly, read in this order:

1. [README.md](/Users/hoschek/unix/cloud/repos/bzfs/README.md) for the product-level view.
2. [README_bzfs_jobrunner.md](/Users/hoschek/unix/cloud/repos/bzfs/README_bzfs_jobrunner.md) for fleet orchestration.
3. [bzfs_main/bzfs.py](/Users/hoschek/unix/cloud/repos/bzfs/bzfs_main/bzfs.py) top docstring for execution flow.
4. [bzfs_main/bzfs_jobrunner.py](/Users/hoschek/unix/cloud/repos/bzfs/bzfs_main/bzfs_jobrunner.py) top docstring for
   orchestration flow.
5. [bzfs_main/replication.py](/Users/hoschek/unix/cloud/repos/bzfs/bzfs_main/replication.py) for replication behavior.
6. [bzfs_main/snapshot_cache.py](/Users/hoschek/unix/cloud/repos/bzfs/bzfs_main/snapshot_cache.py) for the performance
   and correctness trade-offs in snapshot caching.

# Operational Shape of a Real Deployment

Most real deployments look like this:

- source hosts create snapshots locally,
- destination hosts pull replicas from sources,
- each side prunes its own state according to policy,
- monitoring runs separately and alerts when latest or oldest snapshots violate age expectations,
- one shared job config is copied across the fleet and filtered by local hostname at runtime.

This approach avoids a heavyweight always-on coordinator. The system relies on regular scheduled invocations and the
idempotence of the underlying workflows.

# What This System Does Not Try to Be

It is not a new storage layer. It is not an opaque backup appliance. It is not a daemon-heavy control plane with a
database and agents everywhere.

It is a disciplined automation layer over native ZFS behavior.

# The 3 Questions Every New Engineer Will Ask

## 1. Why are there two CLIs instead of one?

Because they solve different problems. `bzfs` is the execution engine for one concrete operation over one source and
destination scope. `bzfs_jobrunner` is the fleet policy layer that repeatedly constructs and launches those operations
across many hosts.

## 2. What is the real unit of correctness: snapshot names, timestamps, or GUIDs?

GUIDs are the strongest identity signal across replication boundaries. Names and times matter for policy, filtering,
retention, and observability, but safe incremental replication ultimately depends on finding a true common basis.

## 3. Why does the project spend so much effort on retries, caching, and resumable state?

Because the target environment is not a toy workflow. Long-running replication over SSH will eventually be interrupted,
large fleets will eventually make repeated `zfs list` calls expensive, and a DR/HA system that cannot recover cleanly
from partial failure is not production-ready.
