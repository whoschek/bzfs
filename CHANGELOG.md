# Changelog

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
