# Substrate

> **The test harness for AI-generated infrastructure.**
> Deterministic. Time-travel debuggable. Cost-visible.

[![CI](https://github.com/scttfrdmn/substrate/actions/workflows/ci.yml/badge.svg)](https://github.com/scttfrdmn/substrate/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/scttfrdmn/substrate)](https://goreportcard.com/report/github.com/scttfrdmn/substrate)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## What is Substrate?

Substrate is an **event-sourced AWS emulator** for testing the infrastructure
code that drives AWS — CloudFormation, CDK, Terraform, and any SDK or CLI call —
**deterministically, offline, and with cost visibility**, before you deploy to a
real account.

It models **what is observable through an AWS API call** — request/response
shapes, resource state and how it transitions over a simulated clock, error
codes, and seedable outcomes — *not* what software inside a resource does. It
never runs your user-data, your Lambda code, an inference, or a training job;
those are recorded as intent with a **seedable** result. That boundary is what
makes every run reproducible: API observations can be recorded as events and
replayed identically, whereas a real workload's timing, scheduling, and I/O
cannot.

**Use it two ways:**
- **As a server** — run `substrate`, point any AWS SDK/CLI at `http://localhost:4566`.
  This is how most consumers use it.
- **As a Go test harness** — `import ".../emulator"`, spin up an in-process server
  or deploy a CloudFormation template directly, no HTTP needed.

## The Problem

```
AI generates infrastructure code → ??? → Deploy to AWS → $$$
                                    ^
                             This is where you find out
```

LocalStack and container-backed emulators run real workloads, so behaviour
depends on wall-clock timing, scheduling, and network — failure and edge-case
paths are hard to trigger and rarely reproduce. Substrate trades workload-internal
fidelity for **determinism**: it makes the API surface accurate and every outcome
seedable, so the rare paths your retry/poll/wait logic exists to handle become
first-class, instant, and repeatable.

## Why Substrate

### 1. Deterministic reproducibility

Every AWS request is an immutable event over a simulated clock. Same inputs +
same seed = same outputs, every time — no flakes, and a failing run replays
identically for debugging.

```go
session, _ := engine.StartRecording(ctx, "test-lambda-timeout")
runTests() // fails
engine.StopRecording(ctx, session)

// Replay it 1000 times — identical failure every time.
for range 1000 {
    results, _ := engine.Replay(ctx, session.StreamID)
}
```

### 2. Time-travel debugging

Step backward through recorded request history and inspect service state at any
point — see exactly where a sequence of API calls diverged from what you expected.

```go
replay := engine.Replay(ctx, "failing-test")
engine.JumpToEvent(ctx, 87)                  // jump to the failure
engine.StepBackward(ctx)                      // step back
state, _ := engine.InspectState(ctx, "iam")   // see what broke
```

### 3. Cost visibility before deploy

Real AWS pricing tracked per operation. Know your monthly bill before it arrives.

```
Total: $1,247.50/month

  S3 PUT:   $875.00  (175M ops @ $0.005/1K)
  Lambda:   $267.50  (10M invocations)

WARNING: High S3 PUT rate — consider batching (save ~99%)
```

### 4. Seedable outcomes (API-surface scope)

Determinism doesn't mean every test sees the same result. Substrate defaults to
the nominal success path; a test **seeds** an alternate outcome — an
`InsufficientInstanceCapacity` on launch, a training job that comes back `Failed`
with a `CapacityError`, a specific query result — and the plugin returns it at
request time, fully reproducibly. The failure, capacity, and timing paths that
are rare or impossible to trigger against real AWS become trivial to test.

## Quick Start (server)

The primary way to use Substrate is as a drop-in AWS endpoint.

### Install

```bash
go install github.com/scttfrdmn/substrate/cmd/substrate@latest
```

Or build from source / run with Docker:

```bash
git clone https://github.com/scttfrdmn/substrate && cd substrate
make build          # produces ./bin/substrate

docker run -p 4566:4566 ghcr.io/scttfrdmn/substrate:latest
```

### Run the server

```bash
substrate server
# Listening on :4566
```

Configuration via `substrate.yaml` or environment variables (see
[`substrate.yaml.example`](substrate.yaml.example)).

### Point your AWS client at it

<details open>
<summary><strong>AWS CLI</strong></summary>

```bash
aws iam create-user --user-name alice \
    --endpoint-url http://localhost:4566 --region us-east-1 --no-sign-request
```

Or set a profile in `~/.aws/config`:

```ini
[profile substrate]
region = us-east-1
endpoint_url = http://localhost:4566
```
</details>

<details>
<summary><strong>Go SDK v2</strong></summary>

```go
cfg, _ := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
    config.WithBaseEndpoint("http://localhost:4566"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
client := iam.NewFromConfig(cfg)
```
</details>

<details>
<summary><strong>Python (boto3)</strong></summary>

```python
import boto3

client = boto3.client(
    "iam", region_name="us-east-1",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test", aws_secret_access_key="test",
)
```
</details>

<details>
<summary><strong>Node.js (AWS SDK v3)</strong></summary>

```javascript
import { IAMClient } from "@aws-sdk/client-iam";

const client = new IAMClient({
  region: "us-east-1",
  endpoint: "http://localhost:4566",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});
```
</details>

## Use as a Go test harness

For Go code, the fastest path is `StartTestServer`, which spins up an in-process
server on a random port and registers a `t.Cleanup` to shut it down.

```go
import "github.com/scttfrdmn/substrate/emulator"

func TestMyInfra(t *testing.T) {
    ts := emulator.StartTestServer(t)

    cfg, _ := config.LoadDefaultConfig(context.Background(),
        config.WithRegion("us-east-1"),
        config.WithBaseEndpoint(ts.URL),
        config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
    )
    // Use cfg with any AWS SDK v2 client...
}
```

Or deploy a CloudFormation template and validate it entirely in-process, no HTTP
server required, via the **Betty** client:

```go
import "github.com/scttfrdmn/substrate/emulator"

store    := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})
state    := emulator.NewMemoryStateManager()
tc       := emulator.NewTimeController(time.Now())
registry := emulator.NewPluginRegistry()

betty := emulator.NewBettyClient(registry, store, state, tc, logger)

result, _ := betty.Deploy(ctx, cfnTemplate, emulator.Intent{MaxCost: 1.0})

session, _ := betty.StartRecording(ctx, "my-test")
// ... run operations against the emulator ...
report, _ := betty.StopRecording(ctx, session)
fmt.Printf("status=%s cost=$%.4f\n", report.PassFail, report.Cost.Total)
```

> The Go import path is `github.com/scttfrdmn/substrate/emulator`. Installing the
> CLI (`.../cmd/substrate@latest`) is unaffected.

See [`examples/betty_workflow/main.go`](examples/betty_workflow/main.go) for a
complete runnable example.

## Supported services

Substrate ships **63 built-in service plugins** spanning compute, storage,
networking, databases, messaging, analytics, ML, security, and management:

- **Compute & containers** — EC2, Lambda, ECS, ECR, Batch, EKS-adjacent
- **Storage & data** — S3, EFS, FSx, DynamoDB, RDS, Redshift, ElastiCache, Timestream
- **Networking** — VPC/EC2, ELBv2, Route 53, CloudFront, API Gateway (REST & HTTP)
- **Messaging & streaming** — SQS, SNS, EventBridge, Kinesis, Firehose, MSK
- **ML & analytics** — SageMaker, Bedrock Runtime, Athena, Glue, EMR Serverless, OpenSearch, QuickSight, HealthOmics
- **Security & identity** — IAM, STS, KMS, Secrets Manager, ACM, Cognito, SSO, WAFv2, RAM
- **Management & cost** — CloudWatch (+ Logs), CloudTrail, Organizations, Budgets, Cost Explorer, Service Quotas, SSM, Health, the CodeSuite, Step Functions, Backup, Transfer

Many integrate with **Betty** for CloudFormation deployment. See the
[Service Reference](docs/services.md) for the authoritative, per-operation list.

### Known limitations

- **Cross-service IAM enforcement** — policies are evaluated for IAM and STS;
  per-operation enforcement for other services is partial.
- **Persistence** — in-memory by default; SQLite available via
  `EventStoreConfig{Backend: "sqlite"}`.
- **Authentication** — SigV4 verification is opt-in (off by default for testing
  ease); enable with `ServerOptions.VerifySignatures = true`.
- **Workload internals are out of scope by design** — Substrate models the API
  surface, not what runs inside a resource (see *What is Substrate?*).

## Status

Current release: **v0.68.0**. See [Releases](https://github.com/scttfrdmn/substrate/releases)
and [CHANGELOG.md](CHANGELOG.md) for full history.

## Documentation

- **[Getting Started](docs/getting-started.md)** — install, first test, 15-minute tutorial
- **[Service Reference](docs/services.md)** — all 63 plugins with operation lists
- **[Testing Guide](docs/testing-guide.md)** — `StartTestServer`, recording/replay, cost assertions
- **[Endpoint Configuration](docs/endpoint-configuration.md)** — SDK and tool configuration

## Development

```bash
make test      # run tests with the race detector
make lint      # golangci-lint
make coverage  # coverage report
make build     # build the substrate binary
make e2e       # end-to-end tests
```

Requirements: Go 1.26+, [golangci-lint](https://golangci-lint.run/).

## Contributing

Issues and pull requests welcome. All work is tracked in
[GitHub Issues](https://github.com/scttfrdmn/substrate/issues) and organised into
[Milestones](https://github.com/scttfrdmn/substrate/milestones). `main` is
protected — changes land via pull request.

## License

Apache 2.0 — see [LICENSE](LICENSE).
