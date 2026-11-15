# Examples

Runnable examples demonstrating Substrate.

## Programs (`go run`)

| Example | What it shows |
|---------|---------------|
| [`custom_plugin`](custom_plugin) | Implementing a minimal custom service plugin (a fictitious "weather" service) and serving it. See also the [contributor guide](../docs/contributing.md). |
| [`validation_workflow`](validation_workflow) | Deploying a CloudFormation template in process and inspecting the result. |

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

## pytest-substrate journey (Python)

[`python/examples/test_journey.py`](../python/examples/test_journey.py) is a complete,
CI-exercised pytest module that tests a small release script against substrate with the
[`pytest-substrate`](../python) plugin. The script uploads an artifact to S3, health-checks a host
through SSM Run Command, records the outcome in an SSM parameter, and opens a maintenance window
with a one-time EventBridge Scheduler schedule. The module shows:

| Test | What it shows |
|------|---------------|
| `test_a_healthy_release_is_recorded` | The `substrate_isolated` fixture (a fresh server per test, boto3 pointed at it through `AWS_ENDPOINT_URL`), the nominal path, and an object stamped with the simulated clock rather than the machine's. |
| `test_a_failed_health_check_is_recorded_with_its_reason` | Seeding a failed command through the control plane (`POST /v1/ssm/command-invocation`), so the script's failure path runs on the first try, and asserting on the state the script wrote. |
| `test_the_maintenance_window_opens_when_the_clock_reaches_it` | `SubstrateServer.set_time` and `advance_time`: an hour passes on the simulated clock in one call, and the schedule fires. |

It uses only the plugin's public fixtures and the documented control plane. Run it with:

```bash
make build
cd python && pip install -e '.[test]' && SUBSTRATE_BINARY=../bin/substrate pytest examples/ -v
```

Terraform and CDK journeys are tracked separately, because they need those toolchains wired into
CI; see the [examples issue](https://github.com/scttfrdmn/substrate/issues/367).
