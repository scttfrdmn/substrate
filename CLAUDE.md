# CLAUDE.md

Substrate — event-sourced AWS emulator for deterministic testing of
AI-generated infrastructure code (CDK/CloudFormation/Terraform).

## Work tracking

All tasks are GitHub Issues assigned to Milestones. Before starting any
non-trivial work, find or create an issue at
https://github.com/scttfrdmn/substrate/issues.

Milestones map directly to semver releases:
https://github.com/scttfrdmn/substrate/milestones

## Code conventions

- Idiomatic Go 1.26; target A+ on https://goreportcard.com/report/github.com/scttfrdmn/substrate
- Every exported symbol requires a doc comment ending with a period.
- Errors: never discard; wrap with context using `fmt.Errorf("…: %w", err)`.
- No `init()` functions; no global mutable state outside of `main`.
- All new packages must live in the root `package substrate` unless there is
  a compelling reason for a sub-package (discuss in an issue first).
- TODOs must reference an issue number: `// TODO(#N): description.`

## Testing

- Run with `make test` (race detector enabled).
- Table-driven tests preferred; test files use `package substrate_test`.
- Aim for >80% coverage; `make coverage` generates the report.
- No test should depend on network access, real AWS, or wall-clock time.

## Changelog

Update `CHANGELOG.md` in every PR under `## [Unreleased]` following the
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format.
Use **Added / Changed / Deprecated / Removed / Fixed / Security** sections.

## Releasing

1. Move `## [Unreleased]` entries to a new `## [vX.Y.Z] - YYYY-MM-DD` section.
2. Add the comparison link at the bottom of `CHANGELOG.md`.
3. Tag: `git tag -s vX.Y.Z -m "vX.Y.Z"` then `git push origin vX.Y.Z`.

## AWS service emulation

When implementing or modifying any AWS service behaviour, **always consult the
official AWS documentation as the authoritative source of truth** — not built-in
knowledge or assumptions. Verify request/response shapes, error codes, pagination
behaviour, and IAM condition keys against the AWS API reference for the relevant
service before writing code or tests.

## Key files

| File | Purpose |
|------|---------|
| `doc.go` | Package documentation |
| `types.go` | Core types (AWSRequest, StateManager, Logger, …) |
| `eventstore.go` | Immutable event log |
| `replay.go` | Replay engine and time-travel debugging |
| `cmd/substrate/main.go` | CLI entry point |
