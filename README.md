# Substrate

> **The test harness for AI-generated infrastructure.**
> Deterministic. Time-travel debuggable. Cost-visible.

[![CI](https://github.com/scttfrdmn/substrate/actions/workflows/ci.yml/badge.svg)](https://github.com/scttfrdmn/substrate/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/scttfrdmn/substrate)](https://goreportcard.com/report/github.com/scttfrdmn/substrate)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Substrate is an event-sourced AWS emulator that validates AI-generated
CloudFormation, CDK, and Terraform before you deploy to AWS.

## The Problem

```
AI generates infrastructure code → ??? → Deploy to AWS → $$$
                                    ^
                             This is where you find out
```

LocalStack hides bugs (no quotas, no realistic consistency, no cost tracking).
Substrate catches them.

## Three Killer Features

### 1. Deterministic Reproducibility

Every AWS request is an immutable event. Same inputs + same seed = same outputs,
every time.

```go
session, _ := engine.StartRecording(ctx, "test-lambda-timeout")
runTests() // fails
engine.StopRecording(ctx, session)

// Replay it 1000 times — identical failure every time.
for range 1000 {
    results, _ := engine.Replay(ctx, session.StreamID)
}
```

### 2. Time-Travel Debugging

Step backward through request history and inspect service state at any point.

```go
replay := engine.Replay(ctx, "failing-test")
engine.JumpToEvent(ctx, 87)       // jump to failure
engine.StepBackward(ctx)          // step back
state, _ := engine.InspectState(ctx, "iam") // see what broke
```

### 3. Cost Visibility Before Deploy

Real AWS pricing tracked per operation. Know your monthly bill before it arrives.

```
Total: $1,247.50/month

  S3 PUT:   $875.00  (175M ops @ $0.005/1K)
  Lambda:   $267.50  (10M invocations)

WARNING: High S3 PUT rate — consider batching (save ~99%)
```

## Getting Started

### Install

```bash
go install github.com/scttfrdmn/substrate/cmd/substrate@latest
```

Or build from source:

```bash
git clone https://github.com/scttfrdmn/substrate
cd substrate
make build          # produces ./bin/substrate
```

### Start the server

```bash
substrate server
# Listening on :4566
```

Configuration via `substrate.yaml` or environment variables (see `substrate.yaml.example`).

### Configure your AWS SDK

#### AWS CLI

```bash
aws iam create-user --user-name alice \
    --endpoint-url http://localhost:4566 \
    --region us-east-1 \
    --no-sign-request
```

Or set permanently in `~/.aws/config`:

```ini
[profile substrate]
region = us-east-1
endpoint_url = http://localhost:4566
```

Then:
```bash
aws --profile substrate iam list-users
```

#### Go SDK v2

```go
import (
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/iam"
)

cfg, _ := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
    config.WithBaseEndpoint("http://localhost:4566"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
        "test", "test", "",
    )),
)
client := iam.NewFromConfig(cfg)
```

#### Python (boto3)

```python
import boto3

client = boto3.client(
    "iam",
    region_name="us-east-1",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)
```

#### Node.js (AWS SDK v3)

```javascript
import { IAMClient } from "@aws-sdk/client-iam";

const client = new IAMClient({
  region: "us-east-1",
  endpoint: "http://localhost:4566",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});
```

### Supported services

| Service | Operations | Status |
|---------|-----------|--------|
| IAM | CreateUser, GetUser, DeleteUser, ListUsers, CreateRole, GetRole, DeleteRole, ListRoles, CreateGroup, GetGroup, DeleteGroup, ListGroups, AttachUserPolicy, DetachUserPolicy, ListAttachedUserPolicies, AttachRolePolicy, DetachRolePolicy, ListAttachedRolePolicies, CreatePolicy, GetPolicy, DeletePolicy, ListPolicies, CreateAccessKey, DeleteAccessKey, ListAccessKeys (25 ops) | ✓ Implemented |
| STS | GetCallerIdentity, AssumeRole, GetSessionToken | ✓ Implemented |
| S3 | CreateBucket, HeadBucket, DeleteBucket, ListBuckets, PutObject, GetObject, HeadObject, DeleteObject, CopyObject, ListObjects, ListObjectsV2, CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListMultipartUploads (16 ops) | ✓ Implemented |
| Betty integration | BettyClient.Deploy (CFN), StartRecording, StopRecording, ValidateRecording, DebugSession | ✓ Implemented |
| Lambda | — | Planned |
| DynamoDB | — | Planned |
| EC2 | — | Planned |

### Known limitations

- **Managed policies**: 47 bundled AWS managed policies are available for attachment but permissions
  are evaluated only within Substrate's IAM engine — not cross-service. No service other than IAM
  and STS is enforced in this release.
- **Persistence**: all state is in-memory; restarting the server resets it.
- **Authentication**: Substrate accepts any AWS credentials without signature verification; use
  `--no-sign-request` or static test credentials.
- **Regions**: single-region only; all resources live in `us-east-1` by default.

---

## Status

| Milestone | Status |
|-----------|--------|
| [v0.1.0 — Event sourcing foundation](https://github.com/scttfrdmn/substrate/milestone/1) | Complete |
| [v0.2.0 — Core server and plugins](https://github.com/scttfrdmn/substrate/milestone/2) | Complete |
| [v0.3.0 — IAM implementation](https://github.com/scttfrdmn/substrate/milestone/3) | Complete |
| [v0.4.0 — Quotas, consistency, costs](https://github.com/scttfrdmn/substrate/milestone/4) | Complete |
| [v0.5.0 — S3 plugin](https://github.com/scttfrdmn/substrate/milestone/5) | Complete |
| [v0.6.0 — Betty integration](https://github.com/scttfrdmn/substrate/milestone/6) | Complete |
| [v1.0.0 — Production release](https://github.com/scttfrdmn/substrate/milestone/7) | In Progress |

## Quick Start

```bash
go get github.com/scttfrdmn/substrate
```

```go
import "github.com/scttfrdmn/substrate"

store    := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
state    := substrate.NewMemoryStateManager()
tc       := substrate.NewTimeController(time.Now())
registry := substrate.NewPluginRegistry()

betty := substrate.NewBettyClient(registry, store, state, tc, logger)

// Deploy a CloudFormation template — all in-process, no HTTP server needed.
result, _ := betty.Deploy(ctx, cfnTemplate, substrate.Intent{MaxCost: 1.0})

// Record and validate operations.
session, _ := betty.StartRecording(ctx, "my-test")
// ... run operations against the emulator ...
report, _ := betty.StopRecording(ctx, session)
fmt.Printf("status=%s cost=$%.4f\n", report.PassFail, report.Cost.Total)
```

See [`examples/betty_workflow/main.go`](examples/betty_workflow/main.go) for a complete runnable example.

## Development

```bash
make test      # run tests with race detector
make lint      # golangci-lint
make coverage  # coverage report
make build     # build the substrate binary
```

Requirements: Go 1.26+, [golangci-lint](https://golangci-lint.run/).

## Contributing

Issues and pull requests welcome. All work is tracked in
[GitHub Issues](https://github.com/scttfrdmn/substrate/issues) and organised
into [Milestones](https://github.com/scttfrdmn/substrate/milestones).

## License

Apache 2.0 — see [LICENSE](LICENSE).
