# CLAUDE.md

Substrate — event-sourced AWS emulator for deterministic testing of
AI-generated infrastructure code (CDK/CloudFormation/Terraform).

## Scope (what substrate models — and what it does not)

**Substrate models what is observable through an AWS API call, not what software
inside a resource does.** A plugin's job is to make every API *observation* a
caller can make accurate, seedable, and time-ordered — never to execute the
workload behind the API.

- In scope: request/response shapes, error codes, resource state and its
  transitions over the simulated clock (e.g. an instance moving
  `pending → running`, a job reporting `Failed` with a seeded reason, a command
  invocation going `Pending → InProgress → Success`), and seedable outcomes that
  let a consumer's poll/retry/wait loop be tested.
- Out of scope: actually running the work — executing user-data or cloud-init,
  running a Lambda's code, performing an inference, running a training job,
  bootstrapping a node. Capture such inputs as recorded intent and expose a
  **seedable** success/failure/completion signal; do not model the internal
  semantics of the workload.

This boundary is also *why* deterministic replay works: API observations can be
recorded as events and replayed identically, whereas resource internals are
nondeterministic (real time, scheduling, I/O). When a proposed feature would
require modeling box-internals, it belongs in a different tool or test tier, not
in substrate. Apply this test before adding behaviour: *is this observable
through an API call, or is it resource-internal?*

**Seeding is how a deterministic emulator produces different results.** A new
operation defaults to its nominal success path; alternate outcomes (errors,
capacity failures, terminal job states, specific result sets, time-ordered
transitions) are exposed as **seedable** values that a plugin reads from a
control-plane endpoint at request time — never as nondeterministic behaviour.
This is what lets a test exercise the rare/slow/failure paths a consumer's
retry/poll/wait loops exist to handle, instantly and reproducibly. Follow the
established seed pattern (`POST`/`DELETE /v1/{service}/...`, keyed by an ID or
`"*"` wildcard; see Bedrock job status, SageMaker training-job status, Athena/
RedshiftData/Timestream results) when adding a new outcome.

The payoff over container- or real-infrastructure-backed approaches is
reproducibility by construction: same inputs (including seeds) → same outputs, a
failing run replays exactly, and state is inspectable at any point in history —
none of which survive real timing, scheduling, and network. The deliberate cost
is workload-internal fidelity, which is out of scope per the boundary above.

Why that reproducibility matters in practice: no test flakes (a red test is a
real signal, not timing noise), exact reproduction of failures from recorded
events (no heisenbugs), time-travel inspection of state at any point, instant and
repeatable coverage of rare paths (capacity/throttle/terminal states via seeds),
and fast, network-free, zero-spend runs suitable for validating IaC before it
touches AWS. Recorded runs can be exported as standalone regression fixtures. See
`doc.go` for the fuller articulation.

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
   **Never move or re-cut a published tag.** Go's checksum database
   (`sum.golang.org`) permanently records a tag's hash on first fetch; moving the
   tag changes the content but not the recorded hash, which breaks `go.sum`
   verification for every consumer (see `SECURITY.md`, #296). Fix any release
   mistake by cutting a new patch version, never by re-tagging.
4. Close the issues the release resolves. A `(#N)` reference in a commit or PR
   **title** only links — it does not auto-close. Use a `Closes #N` / `Fixes #N`
   keyword in the PR **body** (or the merged commit message body), or close the
   issue manually. Only close an issue when every acceptance-criteria checkbox
   is met; if a release ships part of an issue, comment with what shipped vs.
   what remains and leave it open.
5. Close the release's milestone once all its issues are closed.

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
