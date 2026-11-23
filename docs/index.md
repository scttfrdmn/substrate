---
layout: home

hero:
  name: Substrate
  text: The test harness for AI-generated infrastructure
  tagline: An event-sourced AWS emulator for testing CloudFormation, CDK, Terraform, and any SDK or CLI call — deterministically, offline, and with cost visibility, before you deploy.
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started
    - theme: alt
      text: Scope & Philosophy
      link: /scope
    - theme: alt
      text: View on GitHub
      link: https://github.com/scttfrdmn/substrate

features:
  - title: Deterministic reproducibility
    details: Every AWS request is an immutable event over a simulated clock. Same inputs + same seed = same outputs — no flakes, and a failing run replays identically for debugging.
  - title: Time-travel debugging
    details: Step backward through recorded request history and inspect service state at any point, to see exactly where a sequence of API calls diverged from what you expected.
  - title: API-call cost metering
    details: Every emulated API call is priced per operation and summed per service, and Intent MaxCost fails a deploy whose metered cost exceeds a budget. It meters calls, not resource time; a resource-time model is tracked in #1615.
  - title: Seedable outcomes
    details: Substrate models the AWS API surface, not workload internals. Seed an alternate result — a capacity failure, a throttle, a terminal job state — and test the rare paths your retry/poll/wait logic exists to handle, instantly and reproducibly.
---

## What is Substrate?

Substrate is an **event-sourced AWS emulator** for testing the infrastructure
code that drives AWS — CloudFormation, CDK, Terraform, and any SDK or CLI call —
**deterministically, offline, and with cost visibility**, before you deploy to a
real account.

It models **what is observable through an AWS API call** — request/response
shapes, resource state and how it transitions over a simulated clock, error
codes, and seedable outcomes — *not* what software inside a resource does. That
boundary is what makes every run reproducible: API observations can be recorded
as events and replayed identically, whereas a real workload's timing, scheduling,
and I/O cannot. See [Scope & Philosophy](/scope) for the full reasoning.

## Use it three ways

- **As a server** — run `substrate`, point any AWS SDK/CLI at
  `http://localhost:4566`. This is how most consumers use it.
  See [Endpoint Configuration](/endpoint-configuration).
- **As a Go test harness** — `import "github.com/scttfrdmn/substrate/emulator"`,
  spin up an in-process server or deploy a CloudFormation template directly.
  See the [Testing Guide](/testing-guide).
- **As a pytest plugin** — `pytest-substrate` gives a server fixture with
  per-test state reset and boto3 pre-wired. It is installed from source until it is
  published to PyPI ([#1616](https://github.com/scttfrdmn/substrate/issues/1616)).
  See [Getting Started](/getting-started#first-python-test-pytest).

Substrate is equally at home testing human-written and AI-generated
infrastructure — the reproducibility, offline speed, and cost metering matter
either way.

Every built-in service plugin, with the operations it routes, is listed in the
generated [coverage matrix](/services#coverage-matrix).
