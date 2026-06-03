# Scope & Philosophy

Substrate's defining principle — what it models, what it deliberately doesn't,
and why that boundary is also what makes it useful.

## What Substrate models — and what it does not

**Substrate models what is observable through an AWS API call, not what software
inside a resource does.** A plugin's job is to make every API *observation* a
caller can make accurate, seedable, and time-ordered — never to execute the
workload behind the API.

**In scope:** request/response shapes, error codes, resource state and its
transitions over the simulated clock (an instance moving `pending → running`, a
job reporting `Failed` with a seeded reason, a command invocation going
`Pending → InProgress → Success`), and seedable outcomes that let a consumer's
poll/retry/wait loop be tested.

**Out of scope:** actually running the work — executing user-data or cloud-init,
running a Lambda's code, performing an inference, running a training job,
bootstrapping a node. Such inputs are captured as recorded intent with a
**seedable** success/failure/completion signal; the internal semantics of the
workload are not modelled.

This boundary is also *why* deterministic replay works: API observations can be
recorded as events and replayed identically, whereas resource internals are
nondeterministic (real time, scheduling, I/O). The scope boundary and the
deterministic-replay guarantee are the same line viewed from two sides.

## Seeding: determinism without sacrificing coverage

Determinism does not mean every test sees the same result. **Seeding** is the
mechanism that lets a deterministic emulator produce different outcomes on
demand. By default an operation returns its nominal success path; a test seeds an
alternate outcome through a control-plane endpoint, and the plugin reads that
seed at request time. The same launch can therefore be made to return
`InsufficientInstanceCapacity`, a training job to come back `Failed` with a
`CapacityError`, or a query to return a specific result set — each chosen by the
test, each fully reproducible.

Crucially, the failure, capacity, and timing paths a consumer's retry/poll/wait
loops exist to handle are exactly the paths that are rare, slow, or impossible to
trigger on demand against real AWS. Seeding makes them first-class, instant, and
deterministic.

## Why determinism (vs. containers or real infrastructure)

The same testing need is reachable three ways, with very different trade-offs:

- **Real AWS / LocalStack-with-containers** run actual workloads, so behaviour
  depends on wall-clock timing, process scheduling, network, and the live state
  of a remote account. Failure and edge-case paths are hard to trigger and rarely
  reproduce; a flake cannot be replayed.
- **Hand-written mocks** are deterministic but bespoke per test, drift from the
  real API, and cannot model state transitions or be inspected over time.
- **Substrate** records every request as an immutable event over a simulated
  clock, so a run is reproducible by construction.

## What determinism and replay give you

- **No flakes** — the same inputs always produce the same outputs, so a green
  test stays green and a red test is a real signal, not timing noise.
- **Exact reproduction** — a failure replays identically from its recorded
  events; you debug the exact run, not an approximation of it.
- **Time-travel inspection** — step backward through request history and read
  resource state at any point.
- **Testable rare paths** — seeded outcomes make capacity failures, throttling,
  terminal job states, and slow transitions instant and repeatable.
- **Fast and free** — no network, no real account, no provisioning latency or
  spend; suitable for unit tests and tight inner loops.
- **Regression fixtures** — a recorded run can be exported as a standalone test,
  turning a once-seen scenario into a permanent guard.

The deliberate cost is fidelity to workload internals, which is out of scope:
Substrate is the fast, deterministic tier for exercising how code *drives and
reacts to* the AWS API — not for validating what runs inside a resource.
