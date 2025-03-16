# Changelog

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.11.0] - TBD

- [bzfs_jobrunner] Also support replicating snapshots with the same target name to multiple destination hosts. 
This changed the syntax of the `--dst-hosts` and `--retain-dst-targets parameters` to be a dictionary that maps each 
destination hostname to a list of zero or more logical replication target names (the infix portion of a snapshot name). 
To upgrade, change your jobconfig file from
`dst_hosts = {"onsite": "nas", "": "nas"}` to `dst_hosts = {"nas": ["", "onsite"]}` and 
`retain_dst_targets = {"onsite": "nas", "": "nas"}` to `retain_dst_targets = {"nas": ["", "onsite"]}`

## [1.10.0] - March 15, 2025

This release contains some fixes and a lot of new features, including ...

- Improved compat with rsync.net.
- Added daemon support for periodic activities every N milliseconds, including for taking snapshots, replicating and pruning.
- Added the [bzfs_jobrunner](README_bzfs_jobrunner.md) companion program, which is a convenience wrapper around `bzfs` that 
simplifies periodically creating ZFS snapshots, replicating and pruning, across source host and multiple destination hosts, 
using a single shared [jobconfig](bzfs_tests/bzfs_job_example.py) script.
- Added `--create-src-snapshots-*` CLI options for efficiently creating periodic (and adhoc) atomic snapshots of datasets, 
including recursive snapshots.
- Added `--delete-dst-snapshots-except-plan` CLI option to specify retention periods like sanoid, and prune snapshots accordingly.
- Added `--delete-dst-snapshots-except` CLI flag to specify which snapshots to retain instead of which snapshots to delete.
- Added `--include-snapshot-plan` CLI option to specify which periods to replicate.
- Added `--new-snapshot-filter-group` CLI option, which starts a new snapshot filter group containing separate 
`--{include|exclude}-snapshot-*` filter options, which are UNIONized.
- Added `anytime` and `notime` keywords to `--include-snapshot-times-and-ranks`.
- Added `all except` keyword to `--include-snapshot-times-and-ranks`, as a more user-friendly filter syntax to say 
"include all snapshots except the oldest N (or latest N) snapshots".
- Log pv transfer stats even for tiny snapshots.
- Perf: Delete bookmarks in parallel.
- Perf: Use CPU cores more efficiently when creating snapshots (in parallel) and when deleting bookmarks (in parallel) and on 
`--delete-empty-dst-datasets` (in parallel)
- Perf/latency: no need to set up a dedicated TCP connection if no parallel replication is possible.
- For more clarity, renamed `--force-hard` to `--force-destroy-dependents`. `--force-hard` will continue to work as-is for 
now, in deprecated status, but the old name will be completely removed in a future release.
- Use case-sensitive sort order instead of case-insensitive sort order throughout.
- Use hostname without domain name within `--exclude-dataset-property`.
- For better replication performance, changed the default of `bzfs_no_force_convert_I_to_i` from `false` to `true`. If ZFS 
complains with a "cannot hold: permission denied" error, then the fix is for the ZFS administrator to grant ZFS 'hold' 
permission to the user on the source datasets, like so: 'sudo zfs allow -u $SRC_USER send,hold tank1/foo'. Or run with 
'export bzfs_no_force_convert_I_to_i=false'. Also see https://github.com/openzfs/zfs/issues/16394
- Fixed "Too many arguments" error when deleting thousands of snapshots in the same 'zfs destroy' CLI invocation.
- Make 'zfs rollback' work even if the previous 'zfs receive -s' was interrupted.
- Skip partial or bad 'pv' log file lines when calculating stats.
- For the full list of changes, see https://github.com/whoschek/bzfs/compare/v1.9.0...v1.10.0

## [1.9.0] - January 20, 2025

This release contains performance and documentation enhancements as well as bug fixes and new features, including ...

- For replication, periodically prints progress bar, throughput metrics, ETA, etc, to the same console status line (but 
not to the log file), which is helpful if the program runs in an interactive terminal session. The metrics represent 
aggregates over the parallel replication tasks. Example console status line:
```
2025-01-17 01:23:04 [I] zfs sent 41.7 GiB 0:00:46 [963 MiB/s] [907 MiB/s] [==========>  ] 80% ETA 0:00:04 ETA 01:23:08
```
- Fix shutdown for the case where bzfs_reuse_ssh_connection=false
- --delete-dst-datasets: With --recursive, never delete non-selected dataset subtrees or their ancestors.
- Improved latency if there's only a single dataset to replicate over SSH
- Parallel performance: use better heuristic to choose num-datasets-per-thread
- Also run nightly tests with final release of zfs-2.3.0
- For the full list of changes, see https://github.com/whoschek/bzfs/compare/v1.8.0...v1.9.0

## [1.8.0] - January 4, 2025

This release contains performance and documentation enhancements as well as new features, including ...
- Substantially improved SSH connection latency
- Improved latency in local mode
- Parse pv log file correctly even if a locale specific number contains a "," instead of a "." as decimal point
- Use more human-readable formatting for durations and bytes transferred.
- Also run nightly tests on FreeBSD-14.2
- For the full list of changes, see https://github.com/whoschek/bzfs/compare/v1.7.0...v1.8.0

## [1.7.0] -  December 23, 2024

This release contains performance and documentation enhancements as well as new features, including ...
- bzfs now automatically replicates the snapshots of multiple datasets in parallel for best performance. Similarly, it quickly deletes (or compares) snapshots of multiple datasets in parallel.
- Replication and --delete-dst-snapshots: list snapshots in parallel on src and dst.
- Improved reliability of connection resource cleanup.
- bump --force-hard from undocumented to documented feature.
- Logging readability improvements.
- Also run nightly tests on zfs-2.2.7
- For the full list of changes, see https://github.com/whoschek/bzfs/compare/v1.6.0...v1.7.0

## [1.6.0] -  December 2, 2024

### Added
- See https://github.com/whoschek/bzfs/compare/v1.5.0...v1.6.0

## [1.5.0] -  November 21, 2024

### Added
- See https://github.com/whoschek/bzfs/compare/v1.4.0...v1.5.0

## [1.4.0] -  November 12, 2024

### Added
- See https://github.com/whoschek/bzfs/compare/v1.3.0...v1.4.0

## [1.3.0] - November 3, 2024

### Added
- See https://github.com/whoschek/bzfs/compare/v1.2.0...v1.3.0

## [1.2.0] - October 27, 2024

### Added
- See https://github.com/whoschek/bzfs/compare/v1.1.0...v1.2.0

## [1.1.0] - October 9, 2024

### Added
- See https://github.com/whoschek/bzfs/compare/v1.0.0...v1.1.0

## [1.0.0] - October 3, 2024
### Added
- Initial release.
See the README.md and run `bzfs --help` to learn more.

### Fixed
### Changed
