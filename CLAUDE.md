# CLAUDE.md

Substrate — event-sourced AWS emulator for deterministic testing of
AI-generated infrastructure code (CDK/CloudFormation/Terraform).

## Scope (what substrate models — and what it does not)

**Substrate models what is observable through an AWS API call, not what software
inside a resource does.** A plugin's job is to make every API *observation* a
caller can make accurate, seedable, and time-ordered — never to execute the
workload behind the API.

- In scope: request/response shapes, error codes, resource state and its
  transitions (e.g. an instance moving `pending → running`, a job reporting
  `Failed` with a seeded reason, a snapshot reported `pending` for a seeded
  number of observations before it reaches `completed`), and seedable outcomes
  that let a consumer's poll/retry/wait loop be tested. A progression is driven
  by the simulated clock or by a countdown of observations, whichever the
  operation's own reference behaviour makes assertable without wall-clock
  dependence.
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

Merge with `gh pr merge <N> --squash --delete-branch`. **Do not pass
`--delete-branch=false`** — the repository's `delete_branch_on_merge` is `false`, so
the flag is the only thing that removes the branch, and omitting it leaves the merged
branch on the remote indefinitely. That is how 45 merged branches accumulated through
v0.97.0 before being swept; none of them had been kept for a reason. A branch is
recoverable from its PR page after deletion, so keeping it buys nothing that a squash
merge has not already recorded on `main`.

PR conventions, as practiced:

- Branch `<type>/v<milestone>-<area>` (e.g. `fix/v0126-ec2`); title in conventional
  form, naming the issues: `fix(#1153, #1103): …`. One PR usually bundles one
  milestone's work in one area.
- Body: `Fixes #N` lines first (a title reference does not close anything; write
  `Part of #N` plus what remains when an issue ships partly), then one bullet per
  area carrying the reasoning and provenance the CHANGELOG bullet leaves out, the
  routed-operation catalog count change ("from 1,042 to 1,055"), the gates
  run locally, and follow-up issues filed with their numbers.
- A release PR (`chore/release-vX.Y.Z`) changes only `CHANGELOG.md`; versions are
  stamped by ldflags (`emulator/doc.go` stays `"dev"`) and docs carry no pinned
  numbers.
- Adding a plugin or an operation touches a fixed set of tables, each enforced by a
  test or a CI gate; the list and the local `make` targets that reproduce the CI drift
  jobs are in `docs/contributing.md`.

## Code conventions

- Idiomatic Go 1.26; target A+ on https://goreportcard.com/report/github.com/scttfrdmn/substrate
- Every exported symbol requires a doc comment ending with a period.
- Errors: never discard; wrap with context using `fmt.Errorf("…: %w", err)`.
- No `init()` functions; no global mutable state outside of `main`.
- All new packages must live in the root `package substrate` unless there is
  a compelling reason for a sub-package (discuss in an issue first).
- TODOs must reference an issue number: `// TODO(#N): description.`
- Comments describe the code as it is. Do not narrate history: no "before/until/since
  #N", "used to", "previously", "this fix", and no note left where a deleted function
  used to be. History belongs in git, the PR and the CHANGELOG. A trailing `(#N)`
  pointing at the reasoning is fine, and so are AWS source citations and `TODO(#N)`.
  So is a factual note about records an older build wrote, which the code still reads
  ("a record written before #N carries no …"). `make comment-history-check` refuses
  the narrow forms in added lines, and `make doc-links-check` refuses a `[name]` doc
  link that names no identifier.

## Testing

- Run with `make test` (race detector enabled).
- Table-driven tests preferred; test files use `package substrate_test`.
- Aim for >80% coverage; `make coverage` generates the report.
- No test should depend on network access, real AWS, or wall-clock time.

## Changelog

Update `CHANGELOG.md` in every PR under `## [Unreleased]` following the
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format.
Use **Added / Changed / Deprecated / Removed / Fixed / Security** sections, plus
**Compatibility** (below).

An entry is one bullet per issue, one sentence of about 25 words or fewer:

```
- <Service>: <what changed> (#N).
```

For example, `- Lambda: POST /v1/lambda/invoke-result seeds a successful Invoke payload (#1611).`
Add a `### Compatibility` section whenever a consumer must act on upgrading: a removed
API, config key, route or seed, or a changed default. Its bullets take the same
`- <Area>: … (#N).` form and may run longer, because they name exactly what to change
(every removed symbol, a seed body to copy). Say what to do, not why. The
reasoning, provenance and AWS sources go in the PR or issue body, which the bullet's
`(#N)` reaches; the CHANGELOG is the index, not the record. Earlier releases' entries
are longer and stay as they are.

## Releasing

**A release tag is cut only by the steps below, never as a side effect of
merging a feature.** Merging a PR must not create or push a tag. A version tag is
created only after its `## [vX.Y.Z]` CHANGELOG section exists, and its version
must be strictly greater than the latest existing tag (`git tag --sort=-v:refname
| head -1`) — never an ancestor commit or an out-of-order number. (A stray
out-of-order tag, `v0.67.0`, was created this way once; it is burned and
documented as void in `SECURITY.md`.) Derive the next version from the latest tag
at release time rather than trusting a number written here — a pinned number goes
stale silently, which is exactly what `scripts/check-doc-versions.sh` exists to
catch in the docs.

`main` is protected by a GitHub ruleset: all changes land via PR (no direct
pushes), and a separate ruleset makes `refs/tags/v*` immutable (no move/delete).
These enforce the rules below server-side — do not attempt to bypass them.

1. Move `## [Unreleased]` entries to a new `## [vX.Y.Z] - YYYY-MM-DD` section
   (via a PR — `main` cannot be pushed to directly).
2. Add the comparison link at the bottom of `CHANGELOG.md`.
3. After the changelog PR merges, tag the merge commit:
   `git tag -s vX.Y.Z -m "vX.Y.Z"` then `git push origin vX.Y.Z`.
   **Never move or re-cut a published tag.** Go's checksum database
   (`sum.golang.org`) permanently records a tag's hash on first fetch; moving the
   tag changes the content but not the recorded hash, which breaks `go.sum`
   verification for every consumer (see `SECURITY.md`, #296). Fix any release
   mistake by cutting a new patch version, never by re-tagging. The tag ruleset
   will reject a move or delete regardless.
4. Publish a GitHub Release against the tag — the tag alone does not create one,
   and v0.84.0/v0.85.0 shipped without one because this step was unwritten:
   `gh release create vX.Y.Z --verify-tag --latest --title vX.Y.Z --notes-file <file>`.
   Always pass `--verify-tag`, so the command fails rather than creating a tag if
   the tag is missing — `gh release create` will otherwise tag the current branch
   tip, producing an unsigned tag at whatever commit happens to be checked out.
   Create releases in ascending version order, and pass `--latest` on the newest.
   Notes follow the house style: a prose lede naming the release's theme, the
   release's CHANGELOG bullets (one per issue, already terse, so reuse them), a
   `## Provenance` section when any error code comes from observed behaviour
   rather than the API model (drawn from the PR bodies), and a `## Compatibility`
   section stating what a consumer must know before upgrading. Close with the
   CHANGELOG anchor, the compare link, and a line pointing at the release's PRs and
   issues (the milestone page), which carry the reasoning. A release body is
   editable after publication (`gh release edit`), unlike the tag it points at. `make tag-releases-check` asserts this step
   happened; the `Release Audit` workflow runs it daily, so a forgotten Release
   surfaces within a day rather than at the next release.
5. Close the issues the release resolves. A `(#N)` reference in a commit or PR
   **title** only links — it does not auto-close. Use a `Closes #N` / `Fixes #N`
   keyword in the PR **body** (or the merged commit message body), or close the
   issue manually. Only close an issue when every acceptance-criteria checkbox
   is met; if a release ships part of an issue, comment with what shipped vs.
   what remains and leave it open.
6. Close the release's milestone once all its issues are closed. Issues
   deliberately deferred out of the release must be **moved to the next
   milestone**, not closed — comment on each saying why it moved and whether
   anything in the shipped release changed its analysis.

## AWS service emulation

When implementing or modifying any AWS service behaviour, **always consult the
official AWS documentation as the authoritative source of truth** — not built-in
knowledge or assumptions. Verify request/response shapes, error codes, pagination
behaviour, and IAM condition keys against the AWS API reference for the relevant
service before writing code or tests.

**Error message text.** Codes and HTTP statuses must match AWS; message *text* is matched as
you go, not in a sweep (#487). Any change that adds or touches an `AWSError` uses AWS's wording
when a source publishes it: the API reference, a user-guide page, or a documented CLI/SDK example.
Note that source in a comment next to the literal. Where nothing publishes the wording, keep
substrate's own message and say so in the PR body ("the messages are substrate's own"), or
cite observed behaviour (an SDK or Terraform report) under the release's `## Provenance`.
Never invent text that imitates AWS without a source.

## Key files

| File | Purpose |
|------|---------|
| `emulator/doc.go` | Package documentation |
| `emulator/types.go` | Core types (AWSRequest, StateManager, Logger, …) |
| `emulator/eventstore.go` | Immutable event log |
| `emulator/replay.go` | Replay engine and time-travel debugging |
| `emulator/plugins.go` | `RegisterDefaultPlugins` — the plugin registry |
| `docs/services.md` | Service reference index; its coverage matrix is generated (`make docs-reference`) |
| `docs/reference/<plugin>.md` | One hand-written page per plugin; `docs/reference/concepts/` holds the cross-cutting pages |
| `cmd/substrate/main.go` | CLI entry point |
