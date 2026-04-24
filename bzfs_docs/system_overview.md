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

# Why bzfs

ZFS gives operators excellent storage primitives: immutable snapshots, incremental send streams, receive-side rollback
semantics, bookmarks, holds, properties, and delegated administration. Those primitives are deliberately low level. They
do not decide which snapshots should exist, which destinations should receive them, which old state is safe to delete,
which interrupted transfer can be resumed, or how a fleet should keep doing the same work correctly every second or
minute for years.

`bzfs` exists because the difficult part of replication is not the first successful `zfs send`. The difficult part is
the thousandth run, after hosts have rebooted, networks have dropped, retention policy has changed, operators have made
manual repairs, datasets have grown new descendants, and the destination is not quite in the state the next incremental
send expects.

The project turns native ZFS operations into a repeatable disaster-recovery and high-availability workflow:

- make source snapshots on policy,
- replicate selected snapshots to one or more destinations,
- preserve enough ancestry to continue incremental replication safely,
- prune snapshots and bookmarks without destroying the only useful base,
- monitor whether expected snapshots are being successfully created, replicated, and pruned on schedule,
- keep the whole system observable through ordinary CLIs, logs, dry runs, and ZFS state.

The design keeps the system understandable by using ZFS itself as the source of truth. There is no required daemon,
external database, custom replication format, or hidden control plane. The project composes ordinary Unix processes,
`zfs`, `ssh`, and Python code into an idempotent automation layer that can run on minimal hosts.

# The System in One Sentence

`bzfs` is the execution engine that makes one source/destination ZFS snapshot tree converge safely, while
`bzfs_jobrunner` is the fleet scheduler that expands a shared policy file into repeated `bzfs` invocations across many
source and destination hosts.

# The Problem Space

The system is built for environments where ZFS replication is operational infrastructure, not an occasional maintenance
command. Typical deployments use it for backup, disaster recovery, warm standbys, geographically distributed replicas,
removable backup targets, or low-latency replication for selected datasets.

That problem has several separate concerns:

- Snapshot creation: take snapshots at the cadence implied by policy.
- Snapshot selection: include only the datasets and snapshot labels that belong to a given operation.
- Replication: send only the data needed to move the destination from its current state toward the source.
- Retention: delete old snapshots and bookmarks according to policy without breaking future incremental sends.
- Monitoring: alert when the newest or oldest matching snapshot violates the expected age window.
- Recovery: retry, resume, or conservatively fall back after partial failure.

The important point for new engineers is that replication correctness is a state problem, not a command-construction
problem. The code spends most of its care on discovering the real state of source and destination, proving that a send
is safe, and recovering when reality changes underneath a run.

# ZFS Concepts Used by This Project

You do not need to be a ZFS expert to start reading the code, but these concepts explain most design decisions.

## Datasets and Dataset Trees

A ZFS dataset is the unit that contains files, child datasets, snapshots, and properties. A recursive `bzfs` run treats
a root dataset plus selected descendants as a dataset tree. Many bugs in replication tools come from thinking about a
single dataset when the real deployment has a tree whose descendants can appear, disappear, or be filtered
independently.

## Snapshots

A snapshot is an immutable point-in-time view of one dataset. A snapshot name is useful for humans and policy, but the
name alone is not a proof of identity. Two pools can contain snapshots with the same name that do not describe the same
block history.

## GUIDs

ZFS assigns snapshots GUIDs. `bzfs` uses GUIDs when it needs to decide whether source and destination share the same
logical snapshot. Names and timestamps drive filtering, retention, and observability; GUIDs protect the incremental
replication base.

## Creation Time and `createtxg`

`creation` is a UTC timestamp. `createtxg` is the transaction group in which the snapshot was created. `createtxg` is
precise for ordering snapshots within one pool, while `creation` remains meaningful across pools. The implementation is
careful about when each property is the right ordering signal.

## Latest Common Snapshot or Bookmark

Incremental replication needs a common base. If source and destination share snapshot `A`, source can send the changes
from `A` to a later snapshot `B`. `bzfs` searches for the latest common source item, where the item may be a snapshot or
a bookmark, then plans the send steps from that basis.

## Bookmarks

A bookmark is small ZFS metadata that records enough ancestry to serve as the source side of a later incremental send.
It does not contain user data. Bookmarks let the source delete old snapshots earlier while still preserving the ability
to replicate incrementally, as long as the destination still has the corresponding base snapshot.

## Resumable Receive Tokens

When `zfs receive -s` leaves an interrupted receive behind, ZFS can expose a receive-resume token. `bzfs` can use that
token with `zfs send -t` to avoid retransmitting already received bytes. The code must also know when stale resumable
state blocks the next valid receive and should be cleared.

# The Two Executables

The project intentionally has two CLIs because the two layers have different correctness boundaries.

## `bzfs`

`bzfs` is the direct ZFS engine. It can be used alone. It handles:

- parsing one source and one destination scope,
- listing source and destination datasets, snapshots, bookmarks, and properties,
- applying include and exclude filters,
- finding the latest common snapshot or bookmark by GUID,
- deciding whether the next send is full, incremental with `-i`, or incremental with `-I`,
- running `zfs send`, `zfs receive`, `ssh`, and optional helper programs,
- creating snapshots, creating bookmarks, deleting selected snapshots or datasets, comparing trees, and monitoring
  snapshot age,
- retrying transient failures and handling resumable receive state.

If you are debugging whether one dataset should replicate, start with `bzfs`.

## `bzfs_jobrunner`

`bzfs_jobrunner` is the fleet orchestration layer. It is intentionally thinner than `bzfs`. It handles:

- reading one shared Python job configuration,
- selecting the source and destination hosts relevant to this invocation,
- mapping logical snapshot targets such as `onsite` or `us-west` to real destination hosts,
- computing root dataset pairs and destination roots,
- forwarding concrete work to `bzfs`,
- running periodic actions such as create, replicate, prune, and monitor across many hosts.

If you are debugging why a host did or did not run a job, start with `bzfs_jobrunner` and the job config.

# The Shared Job Configuration

The job config is Python because fleet policy is conditional in practice. Hostnames, destination roots, target mappings,
retention windows, and monitoring thresholds often need small computed differences. A Python file keeps that logic
version-controlled and reviewable without adding a custom language.

The example is [bzfs_testbed/bzfs_job_testbed.py](../bzfs_testbed/bzfs_job_testbed.py). It defines the main policy
objects:

- `root_dataset_pairs`: source dataset roots and destination-relative roots.
- `src_hosts`: source hosts that own production datasets.
- `dst_hosts`: destination hosts and the logical targets they should receive.
- `retain_dst_targets`: target mappings used when pruning destination snapshots.
- `dst_root_datasets`: per-destination prefixes for replicated datasets.
- `src_snapshot_plan`, `src_bookmark_plan`, and `dst_snapshot_plan`: retention and creation policy.
- `monitor_snapshot_plan`: freshness and retention-age expectations.

A common deployment copies the same job config to all hosts. Source hosts run creation and source pruning actions.
Destination hosts run replication and destination pruning actions. Monitoring may run on either side or from a central
host. There is no required always-on coordinator.

# The Replication Mental Model

A single replication task is a convergence loop for one source dataset and its matching destination dataset:

1. Resolve the source dataset and destination dataset names.
2. List source snapshots and, when enabled, source bookmarks.
3. List destination snapshots.
4. Filter source items by dataset policy, snapshot regexes, time windows, and rank windows.
5. Compare GUIDs to find the latest common source snapshot or bookmark.
6. If the destination has incompatible newer state, either stop or apply explicitly requested reconciliation.
7. If there is no common base, perform a full send of the oldest selected source snapshot.
8. If there is a common base, compute the smallest safe sequence of incremental send steps.
9. Run `zfs send` and `zfs receive`, with SSH, compression, buffering, progress reporting, and resumable receive support
   when configured and available.
10. Create bookmarks and update conservative caches so future runs can do less work.

The core implementation is split as follows:

- [bzfs_main/bzfs.py](../bzfs_main/bzfs.py): CLI entry point, job setup, task orchestration, filters, creation,
  comparison, monitoring, retries, SSH command execution, and parallel task handling.
- [bzfs_main/replication.py](../bzfs_main/replication.py): full and incremental replication for one dataset.
- [bzfs_main/incremental_send_steps.py](../bzfs_main/incremental_send_steps.py): minimal `-i` and `-I` send-step
  planning, including bookmark and resumable-send limitations.
- [bzfs_main/filter.py](../bzfs_main/filter.py): dataset and snapshot filtering.
- [bzfs_main/snapshot_cache.py](../bzfs_main/snapshot_cache.py): cache fast paths guarded by ZFS properties.
- [bzfs_main/bzfs_jobrunner.py](../bzfs_main/bzfs_jobrunner.py): fleet-level job expansion and subjob coordination.

# Safety Invariants

These are the invariants to protect when changing the code.

## Source Mutation Is Explicit

Replication normally reads from the source. Source-side mutation happens only through explicit actions such as creating
snapshots, creating bookmarks, pruning source snapshots, or pruning source bookmarks.

## Destination Destruction Is Explicit

Normal replication appends newer destination snapshots. Deleting destination snapshots, deleting datasets, rolling back,
or reconciling divergence must be requested through options whose effect can be inspected through dry runs and logs.

## Incremental Sends Require a Proven Base

The latest common snapshot or bookmark is a correctness boundary. If the code cannot prove a common basis, it must not
pretend an incremental send is safe. It should perform a full send when allowed, stop, or ask the configured destructive
reconciliation path to make the destination compatible.

## Retry Must Preserve Idempotence

The next run after a crash, timeout, SSH failure, process kill, or interrupted receive should be able to make progress
without manual repair in ordinary cases. This is why retry logic, receive-resume token handling, and conservative
fallbacks matter.

## Caches Are Evidence, Not Truth

The snapshot cache exists to avoid expensive `zfs list` calls. It is trusted only when live ZFS `snapshots_changed`
values, maturity checks, and cache-internal timestamps agree. A cache miss may cost time; a false cache hit would risk
correctness, so the design prefers fallback.

## Native ZFS Semantics Stay Visible

The system does not serialize hidden replication state into an external database. Operators can inspect ZFS datasets,
snapshots, bookmarks, properties, holds, resume tokens, logs, and dry-run commands directly.

# Performance Model

The project is optimized for large dataset trees and repeated scheduled runs. The main costs are command startup, SSH
latency, `zfs list` latency, send-stream bandwidth, and destination receive time.

Important performance choices:

- work is parallelized across independent datasets where safe,
- command arguments are batched to stay under OS limits,
- source and destination snapshot lists are fetched in parallel where possible,
- `createtxg`, `creation`, and GUID fields are requested only when needed,
- progress from parallel sends is aggregated for operator visibility,
- optional `pv`, `zstd`, and `mbuffer` integrations are auto-detected rather than required,
- `--cache-snapshots` uses ZFS `snapshots_changed` and small inode timestamp files to skip redundant list operations.

Performance features must never weaken the safety model. In this codebase, a fast path should be treated as a proof that
the slow path would have produced the same decision. If that proof is not available, use the slow path.

# Failure Model

The system assumes realistic production failure modes:

- source or destination hosts may reboot,
- SSH sessions may drop,
- cron or systemd may start a later run after an earlier run failed,
- another operator or tool may create or delete snapshots,
- a receive may be left with a resumable token,
- a destination may have snapshots unknown to the source,
- some optional helper tools may not be installed,
- old ZFS versions may lack a feature that newer versions have.

The response should be conservative. `bzfs` should either continue from a known-safe base, resume a transfer that ZFS
shows to be resumable, clear stale receive state when that is the correct recovery, or stop with enough context for an
operator to act.

# What the System Is Not

`bzfs` is not a storage engine, not a backup appliance, not a daemon-first platform, and not an opaque scheduler backed
by a database. It is a disciplined automation layer over native ZFS operations.

That constraint is intentional. It keeps deployments simple, makes dry runs meaningful, and lets an operator reason from
the same commands the system executes.

# How to Read the Repository

Start in this order if you have no context:

1. [README.md](../README.md): user-facing `bzfs` behavior and CLI surface.
2. [README_bzfs_jobrunner.md](../README_bzfs_jobrunner.md): periodic fleet workflow.
3. [bzfs_testbed/bzfs_job_testbed.py](../bzfs_testbed/bzfs_job_testbed.py): a concrete job configuration.
4. [bzfs_main/bzfs.py](../bzfs_main/bzfs.py): direct engine entry point and job execution flow.
5. [bzfs_main/replication.py](../bzfs_main/replication.py): the core replication algorithm.
6. [bzfs_main/incremental_send_steps.py](../bzfs_main/incremental_send_steps.py): send-step planning.
7. [bzfs_main/snapshot_cache.py](../bzfs_main/snapshot_cache.py): cache correctness and performance trade-offs.
8. [bzfs_main/bzfs_jobrunner.py](../bzfs_main/bzfs_jobrunner.py): fleet orchestration.

When changing behavior, read the corresponding tests under [bzfs_tests](../bzfs_tests). Unit tests cover most policy and
planning logic. Integration tests exercise real ZFS behavior where mocks would hide important semantics.

# How to Reason About Changes

Use this checklist before changing replication behavior:

- What source and destination ZFS state can exist before this code runs?
- Which state is authoritative: snapshot name, GUID, creation time, `createtxg`, `snapshots_changed`, or resume token?
- Does the change preserve the latest-common-snapshot invariant?
- What happens if the process dies before, during or after each external command?
- What happens if the next run sees partial state left by this run?
- Does a dry run show enough for an operator to understand the action?
- Does the fast path have the same decision boundary as the slow path?

Most mistakes in this kind of software come from optimizing for the common case before proving the recovery case.

# The 5 Questions Every New Engineer Will Ask

## 1. Why are there two CLIs?

Because they operate at different layers. `bzfs` is the engine for one concrete source/destination operation. It owns
ZFS correctness. `bzfs_jobrunner` is the policy expander for a fleet. It decides which host pairs, dataset roots,
targets, and actions should produce concrete `bzfs` invocations.

## 2. Where does the truth live?

In ZFS. The job config declares desired policy, and cache files may provide performance evidence, but live ZFS state is
authoritative for correctness. When there is disagreement, the code should re-list, re-check GUIDs and properties, or
stop instead of trusting stale local assumptions.

## 3. How does `bzfs` decide what to send?

It lists eligible source snapshots and bookmarks, lists destination snapshots, filters source items by policy, compares
GUIDs to find the latest common base, checks destination compatibility, then chooses a full send or a minimal sequence
of incremental `-i` and `-I` sends. Snapshot names drive policy; GUIDs protect correctness.

## 4. Why does the project spend so much effort on retries, caching, and resumable state?

Because the target environment is not a toy workflow. Long-running replication over SSH will eventually be interrupted,
large fleets will eventually make repeated `zfs list` calls expensive, and a DR/HA system that cannot recover cleanly
from partial failure is not production-ready.

## 5. Where should I start when debugging a failed run?

First look at the log files and decide which layer failed. If the wrong work was selected, inspect the job config and
`bzfs_jobrunner` arguments. If the right work was selected but replication failed, inspect the `bzfs` dry-run output,
source and destination snapshot GUIDs, latest common snapshot or bookmark, destination rollback state, receive-resume
token, and the exact failed `zfs send` / `zfs receive` command.
