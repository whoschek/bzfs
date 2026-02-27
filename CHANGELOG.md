# Changelog

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.19.0] - Not yet released

- [bzfs] [perf] Use better heuristic for `zfs list -t snapshot` minibatch size if parallelism is disabled.
- [bzfs] [perf] Reuse ssh connection even immediately after reboot of operating system.
- [bzfs] [perf] Refresh ssh connection immediately when the expected control socket path is missing or stale, and remove
  stale control socket paths.
- [bzfs] Add test script that uses Lima to locally create a guest Ubuntu VM, then runs the bzfs test suite inside of
  that VM.
- [bzfs] Also run nightly tests on AlmaLinux-10 with zfs-2.4 and `hpnssh`.

## [1.18.0] - February 14, 2026

- [bzfs_jobrunner] Also reject explicit CLI input opts `--include-snapshot-regex`, `--exclude-snapshot-regex`,
  `--include-snapshot-times-and-ranks`.
- [bzfs_jobrunner] docs: Clarify how to apply actions to a subset of hosts via `--src-host` and `--dst-host` CLI
  options.
- [bzfs] docs: Add example output for `--monitor-snapshots` CLI option.
- [bzfs] docs: Add example output for `--compare-snapshot-lists` CLI option.
- [bzfs] docs: Add docs for `--ssh-control-persist-secs` CLI option.
- [bzfs] Bump default for `--ssh-control-persist-secs` to 600 seconds for improved operational stability.
- [bzfs] Make `call_with_retries()` more widely applicable.
- [bzfs] Also run nightly tests on AlmaLinux-10.1 and AlmaLinux-9.7.
- [bzfs] Also run nightly tests on FreeBSD-13.5.
- For the full list of changes, see https://github.com/whoschek/bzfs/compare/v1.17.0...v1.18.0

## [1.17.0] - January 18, 2026

- [bzfs_jobrunner] docs: Improve Getting Started.
- [bzfs_jobrunner,bzfs] Make `--monitor-snapshots` report exit code for worst encountered alert instead of first
  encountered alert.
- [bzfs_jobrunner,bzfs] `--monitor-snapshots`: Add (optional) `oldest_skip_holds` flag to indicate that snapshots that
  carry a `zfs hold` shall be skipped when monitoring the oldest snapshot. The default is to not skip, i.e. the same
  behavior as before this change.
- [bzfs] `--delete-dst-snapshots`: Don't attempt to delete snapshots that carry a `zfs hold`; instead auto-skip them
  without failing.
- [bzfs] Fix snapshot schedule for N-minutely periods where N > 1.
- [bzfs] Make `call_with_retries()` more widely applicable.
- [bzfs] [security] `--ssh-{src|dst}-config-file`: Make it possible to specify that no ssh configuration files shall be
  read.
- [bzfs] [security] Enhance validation of `--ssh-{src|dst}-user` and ZFS property names.
- [bzfs] [security] Make `os.utime()` not follow symlinks for additional safety.
- For the full list of changes, see https://github.com/whoschek/bzfs/compare/v1.16.0...v1.17.0

## [1.16.0] - December 20, 2025

- [bzfs] Include `--zfs-send-program-opts` also when resuming a previously interrupted `zfs receive -s`.
- [bzfs] Fix potential delay of process exit when receiving SIGINT/SIGTERM while spawning a subprocess.
- [bzfs] Log better diagnostics on `subprocess_run()`.
- [bzfs] Make `run_with_retries()` more widely applicable and more performant.
- [bzfs] Make `run_ssh_command()` more widely applicable.
- [bzfs] Make `process_datasets_in_parallel()` more widely applicable.
- [bzfs] Also run nightly tests on zfs-2.4.0 and zfs-2.2.9.
- For the full list of changes, see https://github.com/whoschek/bzfs/compare/v1.15.0...v1.16.0

Removed CLI options that were deprecated in version ≤ 1.12.0:

- [bzfs_jobrunner] Removed deprecated `--jobid` CLI option (was renamed to `--job-run`).
- [bzfs_jobrunner] Removed deprecated `--src-user` and `--dst-user` CLI options (were renamed to `--ssh-src-user` and
  `--ssh-dst-user`).
- [bzfs_jobrunner] Removed deprecated `--replicate=xyz` CLI option (was replaced by `--replicate`).
- [bzfs] Removed deprecated `--create-src-snapshots-enable-snapshots-changed-cache` CLI option (was replaced with
  `--cache-snapshots`).
- [bzfs] Removed deprecated `--cache-snapshots=true|false` CLI option (was replaced with `--cache-snapshots`).
- [bzfs] Removed deprecated `--no-create-bookmark` CLI option (was replaced with `--create-bookmarks=none`).
- [bzfs] Removed deprecated `--force-hard` CLI option (was renamed to `--force-destroy-dependents`).

## [1.15.1] - December 2, 2025

- [bzfs] Include `--zfs-send-program-opts` also when resuming a previously interrupted `zfs receive -s`.

## [1.15.0] - December 1, 2025

- [bzfs_jobrunner] Fixed: In some edge cases, subjobs run in parallel when they should run sequentially.
- [bzfs] [perf] `--cache-snapshots`: Update the cache for the datasets that were actually replicated even if some
  datasets were skipped.
- [bzfs] More reliable retries when connecting to remote server.
- [bzfs] `--cache-snapshots`: Ensure equal snapshots_changed still allows updating cached creation time.
- [bzfs] [security] Properly round-trip backslash characters in ZFS property values.
- [bzfs] [security] Enable use of `doas` as a replacement for `sudo`, via `--sudo-program=doas`
- [bzfs] Decouple `run_ssh_command()` SSH client utility from the rest of the program.
- [bzfs] Decouple `process_datasets_in_parallel()` parallel task tree scheduler utility from the rest of the program.
- [bzfs] Also run nightly tests on FreeBSD-15.0.
- [bzfs] Also run nightly tests on zfs-2.3.5.
- For the full list of changes, see https://github.com/whoschek/bzfs/compare/v1.14.0...v1.15.0

## [1.14.0] - November 7, 2025

- [bzfs_jobrunner] [perf] Improve latency by enabling bzfs_jobrunner to safely execute multiple bzfs jobs in parallel
  within the same Python process (not just in parallel across multiple Python subprocesses).
- [bzfs] [perf] Use better heuristic to choose num-datasets-per-thread when listing snapshots.
- [bzfs] Change default of `--create-bookmarks` from `hourly` to `all` to improve safety.
- [bzfs] [security] Set the umask so intermediate directories created by `os.makedirs()` have stricter permissions.
- [bzfs] [security] Remove the `--log-config-file` and `--log-config-var` CLI options.
- [bzfs] Don't optimize regex if it might contain a pathological character class that contains parenthesis.
- [bzfs] Also run nightly tests on zfs-2.4.0-rcX.
- [bzfs] Also run nightly tests on python-3.15-dev.
- For the full list of changes, see https://github.com/whoschek/bzfs/compare/v1.13.0...v1.14.0

## [1.13.0] - October 11, 2025

- [bzfs_jobrunner] Prevent leakage of argument parser options across subjobs.
- [bzfs] [perf] Reuse SSH connections across zpools to improve latency.
- [bzfs] [perf] Reuse SSH connections on bzfs process startup to improve latency.
- [bzfs] [perf] Estimate num bytes to transfer via 'zfs send' in parallel to improve latency.
- [bzfs] Retry connecting to `sshd` before giving up.
- [bzfs] Add `--retry-initial-max-sleep-secs` CLI option.
- [bzfs] Swallow repeated internal `Broken Pipe` logging messages when the user terminates a shell pipe prematurely.
- [bzfs] Normalize status codes.
- [bzfs] `--cache-snapshots`: exclude label timestamp from hash function.
- [bzfs] `--no-estimate-send-size`: don't auto-disable mbuffer and zstd.
- [bzfs] Make snapshot cache file paths shorter.
- [bzfs] [security] Enhance validation of file permissions.
- [bzfs] Also run nightly tests on production releases of python-3.14.
- [bzfs] Remove support for python-3.8 as it has been officially EOL'd since Oct 10, 2024.
- [bzfs] Remove support for Solaris (legacy).
- For the full list of changes, see https://github.com/whoschek/bzfs/compare/v1.12.0...v1.13.0

## [1.12.0] - September 17, 2025

- [bzfs_jobrunner] Added ability to [bzfs_jobrunner](README_bzfs_jobrunner.md) to replicate across a fleet of N source
  hosts and M destination hosts, using the same single shared [jobconfig](bzfs_tests/bzfs_job_example.py) script. For
  example, this simplifies the deployment of an efficient geo-replicated backup service where each of the M destination
  hosts is located in a separate geographic region and pulls replicas from (the same set of) N source hosts. It also
  simplifies low latency replication from a primary to a secondary or to M read replicas, or backup to removable drives,
  etc.
- [bzfs_jobrunner] Added example for how to force the use of a separate destination root dataset per source host.
- [bzfs_jobrunner] Added name of localhost to log file name suffix.
- [bzfs_jobrunner] Also log subjobs that were skipped (because a prior subjob failed).
- [bzfs_jobrunner] Added `--jobrunner-dryrun` and `--jobrunner-log-level` CLI options.
- [bzfs_jobrunner] Added `--jitter` CLI option to randomize job start time and host order to avoid potential thundering
  herd problems in large distributed systems.
- [bzfs_jobrunner] Added timeout parameter.
- [bzfs_jobrunner] Added option to customize number of cycles in monitor_snapshot_plan.
- [bzfs_jobrunner] Added '[bzfs_jobrunner]' tag to log messages.
- [bzfs_jobrunner] Added more input validation.
- [bzfs_jobrunner] Added `--ssh-{src|dst}-port` and `--ssh-{src|dst}-config-file` CLI options.
- [bzfs_jobrunner] Replaced the `--jobid` CLI option with `--job-run` and added the (required) `--job-id` CLI option,
  which is forwarded to bzfs. The old `--jobid` option will continue to work as-is for now, in deprecated status, but
  the old name will be completely removed in a future release.
- [bzfs_jobrunner] There's no need anymore to specify an argument to `--replicate`. For the time being the corresponding
  mode argument remains available in deprecated status but is actually ignored. The argument will be removed in a future
  release.
- [bzfs_jobrunner] Renamed `--src-user` and `--dst-user` to `--ssh-src-user` and `--ssh-dst-user`. The old names will
  continue to work as-is for now, in deprecated status, but the old names will be completely removed in a future
  release.
- [bzfs_jobrunner] Promoted `bzfs_jobrunner` from work-in-progress to stable status.
- [bzfs] Fixed: Error "zfs CLI is not available on dst host: localhost" if using pull-push mode with dummy dataset.
- [bzfs] Fixed: Use uid instead of euid in line with ssh convention.
- [bzfs] Promoted `--zfs-recv-o-*` and `--zfs-recv-x-*` options to stable state.
- [bzfs] Find latest common snapshot now even among non-selected snapshots.
- [bzfs] Also support `--delete-dst-snapshots-except` if source is not a dummy.
- [bzfs] Replaced `--create-src-snapshots-enable-snapshots-changed-cache` CLI option with `--cache-snapshots`. The old
  flag will remain available in deprecated state for the time being (yet has no effect anymore), and will be removed in
  a future release.
- [bzfs] Fixed `--cache-snapshots` such that it now *simply works*.
- [bzfs] [perf] Made `--cache-snapshots` also boost the performance of replication and `--monitor-snapshots`.
- [bzfs] [perf] `--cache-snapshots`: No need to require the `extensible_dataset` ZFS feature to enable caching.
- [bzfs] Replaced the `--no-create-bookmarks` CLI option with `--create-bookmarks=none` and added
  `--create-bookmarks=hourly` (the default), `--create-bookmarks=minutely`, `--create-bookmarks=secondly` and
  `--create-bookmarks=all`. The old `--no-create-bookmarks` will continue to work as-is for now, in deprecated status,
  but the old name will be completely removed in a future release.
- [bzfs] Log more detailed diagnostics on `--monitor-snapshots`.
- [bzfs] Added a bash completion script such that typing bzfs SPACE TAB or bzfs_jobrunner TAB will auto-complete all
  flags.
- [bzfs] [perf] Auto-disable mbuffer and compression-on-the-wire if replicating over the loopback address.
- [bzfs] [perf] Detect ZFS features and system capabilities on src+dst in parallel.
- [bzfs] [perf] Create bookmarks in parallel.
- [bzfs] Fixed progress reporting when using 'pv' with French locale and other international locales.
- [bzfs] On SIGTERM, send signal also to descendant processes to also terminate descendants.
- [bzfs] [security] Added `--preserve-properties` CLI option which preserves the current value of ZFS properties with
  the given names on the destination datasets on replication.
- [bzfs] [security] Nomore include `--props` in `zfs send` command by default; instead the new default is to merely copy
  a whitelist of safe dataset properties (if locally set) from src dataset to dst dataset when doing a 'full send', via
  new defaults for `--zfs-recv-o-include-regex` and `--zfs-recv-o-include-targets`. If you'd like to continue to use the
  old behavior, manually set `--zfs-send-program-opts="--props --raw --compressed"` and `--zfs-recv-o-include-regex`
  (without any regex) and `-zfs-recv-o-targets=full+incremental`.
- [bzfs] [security] Removed CLI options `--ssh-{src|dst}-private-key`, `--ssh-{src|dst}-extra-opt(s)`, as it is safer to
  specify these options via `--ssh-{src|dst}-config-file` in the ssh client config file.
- [bzfs] [security] Create lock files in a private, non-world-writable directory, not in the system tmp dir.
- [bzfs] [security] Enhanced validation of CLI options.
- [bzfs] [security] Tightly constrain name of helper programs.
- [bzfs] [security] Refuse to follow symlinks.
- [bzfs] [security] Only give permissions to access log dirs to the Unix user (not the Unix group).
- [bzfs] [security] To help with debugging ssh issues, enable `ssh -v` mode when using `bzfs -v -v -v` mode.
- [bzfs] [security] Capped max number of threads to help guard against DoS.
- Added [![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/whoschek/bzfs) link to an AI that can
  answer questions about the bzfs codebase.
- Added instructions for AI Agents.
- Use standard python module imports throughout to improve ease of use of the (refactored) codebase. Installation via
  pip remains unchanged. Optional system installation from the git repo is now done by adding symlinks to the startup
  shell script, [like so](README.md#Installation).
- Added script to easily run tests on remote host.
- Run nightly tests also on zfs-2.2.8 and zfs-2.3.4
- Run nightly tests also on FreeBSD-14.3
- For the full list of changes, see https://github.com/whoschek/bzfs/compare/v1.11.0...v1.12.0

## [1.11.0] - March 26, 2025

- [bzfs_jobrunner] Added `--monitor-snapshot-plan` CLI option, which alerts the user if the ZFS 'creation' time property
  of the latest or oldest snapshot for any specified snapshot pattern within the selected datasets is too old wrt. the
  specified age limit. The purpose is to check if snapshots are successfully taken on schedule, successfully replicated
  on schedule, and successfully pruned on schedule. See the [jobconfig](bzfs_tests/bzfs_job_example.py) script for an
  example.
- [bzfs_jobrunner] Also support replicating snapshots with the same target name to multiple destination hosts. This
  changed the syntax of the `--dst-hosts` and `--retain-dst-targets` parameters to be a dictionary that maps each
  destination hostname to a list of zero or more logical replication target names (the infix portion of a snapshot
  name). To upgrade, change your [jobconfig](bzfs_tests/bzfs_job_example.py) script from something like
  `dst_hosts = {"onsite": "nas", "": "nas"}` to `dst_hosts = {"nas": ["", "onsite"]}` and from
  `retain_dst_targets = {"onsite": "nas", "": "nas"}` to `retain_dst_targets = {"nas": ["", "onsite"]}`
- [bzfs_jobrunner] The [jobconfig](bzfs_tests/bzfs_job_example.py) script has changed to now use the
  `--root-dataset-pairs` CLI option, in order to support options of the form
  `extra_args += ["--zfs-send-program-opts=--props --raw --compressed"]`. To upgrade, change your jobconfig script from
  ` ["--"] + root_dataset_pairs` to `["--root-dataset-pairs"] + root_dataset_pairs`.
- [bzfs_jobrunner] Added `--jobid` option to specify a job identifier that shall be included in the log file name
  suffix.
- Added `--log-subdir {daily,hourly,minutely}` CLI option.
- Improved startup latency.
- Exclude parent processes from process group termination.
- Nomore support python-3.7 as it has been officially EOL'd since June 2023.
- For the full list of changes, see https://github.com/whoschek/bzfs/compare/v1.10.0...v1.11.0

## [1.10.0] - March 15, 2025

This release contains some fixes and a lot of new features, including ...

- Improved compat with rsync.net.
- Added daemon support for periodic activities every N milliseconds, including for taking snapshots, replicating and
  pruning.
- Added the [bzfs_jobrunner](README_bzfs_jobrunner.md) companion program, which is a convenience wrapper around `bzfs`
  that simplifies periodically creating ZFS snapshots, replicating and pruning, across source host and multiple
  destination hosts, using a single shared [jobconfig](bzfs_tests/bzfs_job_example.py) script.
- Added `--create-src-snapshots-*` CLI options for efficiently creating periodic (and adhoc) atomic snapshots of
  datasets, including recursive snapshots.
- Added `--delete-dst-snapshots-except-plan` CLI option to specify retention periods like sanoid, and prune snapshots
  accordingly.
- Added `--delete-dst-snapshots-except` CLI flag to specify which snapshots to retain instead of which snapshots to
  delete.
- Added `--include-snapshot-plan` CLI option to specify which periods to replicate.
- Added `--new-snapshot-filter-group` CLI option, which starts a new snapshot filter group containing separate
  `-- {include|exclude}-snapshot-*` filter options, which are UNION-ized.
- Added `anytime` and `notime` keywords to `--include-snapshot-times-and-ranks`.
- Added `all except` keyword to `--include-snapshot-times-and-ranks`, as a more user-friendly filter syntax to say
  "include all snapshots except the oldest N (or latest N) snapshots".
- Log pv transfer stats even for tiny snapshots.
- Perf: Delete bookmarks in parallel.
- Perf: Use CPU cores more efficiently when creating snapshots (in parallel) and when deleting bookmarks (in parallel)
  and on `--delete-empty-dst-datasets` (in parallel)
- Perf/latency: no need to set up a dedicated TCP connection if no parallel replication is possible.
- For more clarity, renamed `--force-hard` to `--force-destroy-dependents`. `--force-hard` will continue to work as-is
  for now, in deprecated status, but the old name will be completely removed in a future release.
- Use case-sensitive sort order instead of case-insensitive sort order throughout.
- Use hostname without domain name within `--exclude-dataset-property`.
- For better replication performance, changed the default of `bzfs_no_force_convert_I_to_i` from `false` to `true`. If
  ZFS complains with a "cannot hold: permission denied" error, then the fix is for the ZFS administrator to grant ZFS
  'hold' permission to the user on the source datasets, like so: 'sudo zfs allow -u $SRC_USER send,hold tank1/foo'. Or
  run with 'export bzfs_no_force_convert_I_to_i=false'. Also see https://github.com/openzfs/zfs/issues/16394
- Fixed "Too many arguments" error when deleting thousands of snapshots in the same 'zfs destroy' CLI invocation.
- Make 'zfs rollback' work even if the previous 'zfs receive -s' was interrupted.
- Skip partial or bad 'pv' log file lines when calculating stats.
- For the full list of changes, see https://github.com/whoschek/bzfs/compare/v1.9.0...v1.10.0

## [1.9.0] - January 20, 2025

This release contains performance and documentation enhancements as well as bug fixes and new features, including ...

- For replication, periodically prints progress bar, throughput metrics, ETA, etc, to the same console status line (but
  not to the log file), which is helpful if the program runs in an interactive terminal session. The metrics represent
  aggregates over the parallel replication tasks. Example console status line:
  `2025-01-17 01:23:04[I] zfs sent 41.7 GiB 0:00:46 [963 MiB/s] [907 MiB/s] [==========>  ] 80% ETA 0:00:04 ETA 01:23:08`
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

## [1.7.0] - December 23, 2024

This release contains performance and documentation enhancements as well as new features, including ...

- bzfs now automatically replicates the snapshots of multiple datasets in parallel for best performance. Similarly, it
  quickly deletes (or compares) snapshots of multiple datasets in parallel.
- Replication and --delete-dst-snapshots: list snapshots in parallel on src and dst.
- Improved reliability of connection resource cleanup.
- bump --force-hard from undocumented to documented feature.
- Logging readability improvements.
- Also run nightly tests on zfs-2.2.7
- For the full list of changes, see https://github.com/whoschek/bzfs/compare/v1.6.0...v1.7.0

## [1.6.0] - December 2, 2024

### Added

- See https://github.com/whoschek/bzfs/compare/v1.5.0...v1.6.0

## [1.5.0] - November 21, 2024

### Added

- See https://github.com/whoschek/bzfs/compare/v1.4.0...v1.5.0

## [1.4.0] - November 12, 2024

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

- Initial release. See the README.md and run `bzfs --help` to learn more.

### Fixed

### Changed
