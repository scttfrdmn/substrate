# Examples

Runnable examples demonstrating Substrate.

## Programs (`go run`)

| Example | What it shows |
|---------|---------------|
| [`custom_plugin`](custom_plugin) | Implementing a minimal custom service plugin (a fictitious "weather" service) and serving it. See also the [contributor guide](../docs/contributing.md). |
| [`betty_workflow`](betty_workflow) | Deploying a CloudFormation template through Betty and inspecting the result. |

Run either with, e.g., `go run ./examples/custom_plugin`.

## Journey tests (differentiator walk-throughs)

The [`test/e2e`](../test/e2e) module contains runnable, CI-exercised "journey"
tests that demonstrate Substrate's distinguishing features end-to-end with the
real AWS SDK v2. Each starts its own in-process server via
`emulator.StartTestServer`, so it reads as a self-contained example:

| Journey | File | What it shows |
|---------|------|---------------|
| Seeded throttling → retry | `journey_throttling_test.go` | Seed a throttling fault through the HTTP control plane and observe the SDK surface it, then clear it and succeed — the failure path a retry loop exists to handle, fired deterministically. |
| Record & replay | `journey_replay_test.go` | Record a session of AWS calls as an immutable event stream and replay it deterministically through the plugin registry. |
| Cost gate | `journey_cost_test.go` | Run a workload, read the recorded cost from the event store, and fail if it exceeds a budget — a machine-gradeable guardrail before any real spend. |
| Time-travel lifecycle | `journey_timetravel_test.go` | Pin and advance the simulated clock, and observe time-dependent state (object `LastModified`) move with it — no real waiting. |

Run them with:

```bash
cd test/e2e && go test -run TestJourney -v
```

## Planned

Complete Terraform, CDK, and `pytest-substrate` end-to-end journeys are tracked
separately (they need those external toolchains wired into CI) — see the
[examples issue](https://github.com/scttfrdmn/substrate/issues/367).
