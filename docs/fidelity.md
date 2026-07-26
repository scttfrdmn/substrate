# Compatibility & Fidelity Policy

This page defines what "supported" means for a service in Substrate, how much
fidelity to expect, and how those decisions are made. It complements
[Scope & Philosophy](/scope), which draws the hard boundary between API
observations (in scope) and workload internals (out of scope).

## How fidelity is decided

Every modelled behaviour is verified against the **official AWS API reference**
for the service — request/response shapes, error codes, pagination, and IAM
condition keys — not against built-in assumptions. When Substrate's behaviour
intentionally differs from real AWS, that divergence is documented alongside the
behaviour and its rationale.

The guiding question for any addition is the scope test:

> *Is this observable through an API call, or is it resource-internal?*

If it is resource-internal (running user code, performing an inference,
bootstrapping a node), Substrate does not execute it; it captures the input as
recorded intent and exposes a **seedable** completion signal instead.

## Fidelity levels

A given plugin/operation sits at one or more of these levels. They are
descriptive, not a ranking — a serial service that never needs lifecycle
transitions is not "worse" than a stateful one.

| Level | Meaning |
|-------|---------|
| **Implemented** | The nominal success path is modelled: valid requests return AWS-shaped responses. |
| **Partial** | A subset of the operation's parameters or response fields is modelled. Unmodelled fields are omitted rather than faked. |
| **Fault-aware** | Alternate outcomes (errors, capacity/throttle failures, terminal states) can be **seeded** via a control-plane endpoint, so a caller's retry/poll/wait loop can be exercised deterministically. |
| **Stateful** | Cross-request lifecycle is modelled — a resource transitions through states over the simulated clock (e.g. `pending → running`, `Pending → InProgress → Success`). |
| **CFN-supported** | Betty models the relevant CloudFormation resource types for deploy/drift. |
| **Pricing-supported** | Operations carry cost data, so cost summaries and budget gates work. |

The per-service sections of the [Service Reference](/services) note which of
these apply, and the generated coverage matrix lists every registered plugin.

## What "not yet supported" means

- **An operation absent from a plugin** returns an `UnsupportedOperation`-style
  error, not a silent success. If you need it, open an issue.
- **A plugin absent from the registry** means that service is not emulated yet.
  The live plugin list is always available from the `/ready` endpoint.
- **A field you don't see** in a response is not modelled; Substrate omits
  unmodelled fields rather than returning fabricated values.

## Stability

- The HTTP wire contract and the `cmd/substrate` CLI are treated as stable
  surfaces; breaking changes go through the changelog and semver.
- The Go library (`package emulator`) is pre-1.0 and may evolve; pin a version
  and consult the [changelog](https://github.com/scttfrdmn/substrate/blob/main/CHANGELOG.md).
- New fidelity is added incrementally and driven by
  [issues](https://github.com/scttfrdmn/substrate/issues). Requesting a specific
  operation, field, or seedable outcome is the fastest way to get it modelled.

## Requesting or improving fidelity

Open an issue describing the operation and the observable behaviour you need
(including any error/timing path). If you want to implement it yourself, see the
[Contributing a Service Plugin](/contributing) guide.
