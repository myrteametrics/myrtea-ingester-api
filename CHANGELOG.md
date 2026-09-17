# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v5.3.4] - 2026-09-17

### Changed

- Bumped myrtea-sdk to v5.4.6

### Internal

- Updated CODEOWNERS to include @Ismail731404

## [v5.3.3] - 2026-05-28

### Added

- `AppendOnly` option to skip the mget lookup and merge step for direct document insertion (#36)
- Support for parallelized append-only worker ID retrieval without requiring a routing key

### Changed

- Refactored merge configuration handling for non-append-only requests and document indexing logic
- Refactored flush timer logic to prevent data corruption during merges

### Fixed

- Enforce non-empty document ID for regular bulk index operations to prevent anonymous inserts
- Improved error handling for non-append-only requests

## [v5.3.2] - 2026-04-07

### Changed

- Bumped Go version to 1.26
- Renamed default branch from `master` to `main` and updated CI actions (#35)
- Tidied up dependencies (#34)

### Fixed

- Fixed a panic on Elasticsearch timeout

## [v5.3.1] - 2025-12-04

### Changed

- Bumped myrtea-sdk to v5.3.6 to support new merge configuration (#33)

## [v5.3.0] - 2025-11-28

### Added

- Elasticsearch v8 authentication support (#32)

### Changed

- Migrated to Elasticsearch v8 (#32)
- Bumped Go version and various dependencies, updated linter (#30)

## [v5.2.3] - 2025-01-29

### Security

- Bumped dependencies to fix security issues (#28)

## [v5.2.2] - 2024-10-11

### Added

- Added custom errors for ingest requests (#27)

### Changed

- Refuse empty doctype ingest requests (#27)

## [v5.2.1] - 2024-09-05

### Added

- Optimisations for indirect ingestion (#24)

### Changed

- Removed the Elasticsearch v6 implementation and upgraded myrtea-sdk to v5 (#25)

### Fixed

- Fixed myrtea-sdk version (#26)

## [v5.2.0] - 2024-04-24

### Changed

- Bumped myrtea-sdk to v4.6.0 (#23)
- Improved documentation and added more metrics

## [v5.1.9] - 2024-03-11

### Changed

- Bumped dependency versions (#21)

## [v5.1.8] - 2023-11-24

### Added

- Bulk insert histogram and adjusted duration buckets
- More metrics for the ingester (#18)

### Changed

- Bumped myrtea-sdk for performance improvements
- Changed config keys from variables to methods

### Fixed

- Better logs for bulkIndex error item samples

## [v5.1.5] - 2023-09-25

### Fixed

- Fixed Elasticsearch v6 ingester compatibility (#15)
- Fixed injector re-indexing of existing documents (#16)

## [v5.1.4] - 2023-08-07

### Fixed

- Fixed `applyMerges` invalid data filling when a document is missing

## [v5.1.3] - 2023-08-01

### Added

- Improved error logs on bulk indexing (#12)

### Changed

- Refactored CI toolbox and upgraded to Go 1.20

## [v5.1.2] - 2023-06-12

### Fixed

- Fixed desync in multi-get (#11)

## [v5.1.1] - 2023-06-08

### Fixed

- Fixed invalid ES8 unmarshal of multi-get response (#10)

## [v5.1.0] - 2023-04-04

### Added

- Support for Elasticsearch v7 and time-based ingestion (#7)

## [v5.0.7] - 2023-01-25

### Added

- More logging to help debug a potential deadlock inside worker `bulkChainUpdate()`

## [v5.0.6] - 2023-01-11

### Changed

- Refactored router using the SDK

## [v5.0.5] - 2023-01-10

### Fixed

- Minor fix in production logging environment variable

## [v5.0.4] - 2023-01-09

### Added

- Logger production configuration

## [v5.0.3] - 2022-12-15

### Added

- Support for unbuffered channel on worker

### Changed

- Internal timeout review

## [v5.0.2] - 2022-12-12

### Changed

- Refactored worker chained update with cleaner code and better timeout management

## [v5.0.1] - 2022-12-05

### Added

- More metrics on workers

## [v5.0.0] - 2022-11-28

### Added

- Support for backpressure mechanism (#5)

## [v4.1.3] - 2022-11-17

### Changed

- Upgraded Alpine base image version

### Fixed

- Fixed Dockerfile for the new Alpine `apk` syntax

## [v4.1.2] - 2022-04-26

### Added

- New `ForceUpdate` merge config

### Changed

- Upgraded myrtea-sdk to the latest version

## [v4.1.1] - 2022-01-11

### Added

- Support for field slice merge with deduplication

### Fixed

- Fixed Dockerfile for a fresh initial build

## [v4.1.0] - 2021-10-15

### Added

- Initial public release of the Myrtea Ingester API
- Validation of empty values when applying `FieldReplace`

### Fixed

- Project naming convention for GitHub integration
