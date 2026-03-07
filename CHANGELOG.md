# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `EventStore`: immutable event log with stream grouping, filtering, and snapshots.
- `ReplayEngine`: deterministic stream replay with time-travel debugging (step
  forward/backward, jump-to-event, breakpoints, state inspection).
- `RecordingSession`: named test-recording sessions.
- Core types: `AWSRequest`, `AWSResponse`, `AWSError`, `RequestContext`,
  `Principal`, `StateManager`, `TimeController`, `Logger`, `Plugin`,
  `PluginConfig`, `PluginRegistry`.
- `JSONSerializer` for event persistence.
- Initial project structure, CI workflow, and tooling.

[Unreleased]: https://github.com/scttfrdmn/substrate/commits/main
