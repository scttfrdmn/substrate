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

Or run with Docker:

```bash
docker run -p 4566:4566 ghcr.io/scttfrdmn/substrate:latest
```

### Start the server

```bash
substrate server
# Listening on :4566
```

Configuration via `substrate.yaml` or environment variables (see `substrate.yaml.example`).

### Use in Go tests (recommended)

The fastest way to test Go code against Substrate is `StartTestServer`, which
spins up an in-process server on a random port and registers a `t.Cleanup` to
shut it down automatically:

```go
func TestMyInfra(t *testing.T) {
    ts := substrate.StartTestServer(t)
    defer ts.Close()

    cfg, _ := config.LoadDefaultConfig(context.Background(),
        config.WithRegion("us-east-1"),
        config.WithBaseEndpoint(ts.URL),
        config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
    )
    // Use cfg with any AWS SDK v2 client...
}
```

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

| Service | Protocol | Key Operations | Betty CFN Types |
|---------|----------|----------------|-----------------|
| IAM | Query | CreateUser, CreateRole, CreatePolicy, AttachRolePolicy (25 ops) | AWS::IAM::Role, AWS::IAM::Policy |
| STS | Query | GetCallerIdentity, AssumeRole, GetSessionToken | — |
| S3 | REST/XML | CreateBucket, PutObject, GetObject, ListObjectsV2, multipart (16 ops) | AWS::S3::Bucket |
| Lambda | REST/JSON | CreateFunction, InvokeFunction, UpdateFunctionCode (12 ops) | AWS::Lambda::Function |
| SQS | Query | CreateQueue, SendMessage, ReceiveMessage, DeleteMessage (10 ops) | AWS::SQS::Queue |
| DynamoDB | JSON | CreateTable, PutItem, GetItem, Query, Scan, UpdateItem (15 ops) | AWS::DynamoDB::Table |
| EC2 | Query | RunInstances, DescribeInstances, CreateVpc, CreateSubnet (20 ops) | AWS::EC2::VPC, AWS::EC2::Instance |
| ELB v2 | Query | CreateLoadBalancer, CreateTargetGroup, CreateListener (10 ops) | AWS::ElasticLoadBalancingV2::LoadBalancer |
| Route 53 | REST/XML | CreateHostedZone, ChangeResourceRecordSets (6 ops) | AWS::Route53::HostedZone |
| Resource Groups Tagging | JSON | GetResources, TagResources, UntagResources | — |
| SNS | Query | CreateTopic, Publish, Subscribe, Unsubscribe (10 ops) | AWS::SNS::Topic |
| Secrets Manager | JSON | CreateSecret, GetSecretValue, PutSecretValue (8 ops) | AWS::SecretsManager::Secret |
| SSM Parameter Store | JSON | GetParameter, PutParameter, DeleteParameter (6 ops) | AWS::SSM::Parameter |
| KMS | JSON | CreateKey, Encrypt, Decrypt, GenerateDataKey (8 ops) | AWS::KMS::Key |
| CloudWatch Logs | JSON | CreateLogGroup, CreateLogStream, PutLogEvents (8 ops) | AWS::Logs::LogGroup |
| EventBridge | JSON | PutEvents, CreateEventBus, PutRule (8 ops) | AWS::Events::Rule |
| CloudWatch | Query | PutMetricAlarm, GetMetricStatistics, DescribeAlarms (6 ops) | AWS::CloudWatch::Alarm |
| ACM | JSON | RequestCertificate, DescribeCertificate, DeleteCertificate (5 ops) | AWS::CertificateManager::Certificate |
| API Gateway (REST) | REST/JSON | CreateRestApi, CreateResource, PutMethod, CreateDeployment (12 ops) | AWS::ApiGateway::RestApi |
| API Gateway v2 (HTTP) | REST/JSON | CreateApi, CreateRoute, CreateIntegration, CreateStage (8 ops) | AWS::ApiGatewayV2::Api |
| Step Functions | JSON | CreateStateMachine, StartExecution, DescribeExecution (6 ops) | AWS::StepFunctions::StateMachine |
| ECR | JSON | CreateRepository, PutImage, GetAuthorizationToken (6 ops) | AWS::ECR::Repository |
| ECS | JSON | CreateCluster, CreateService, RegisterTaskDefinition, RunTask (10 ops) | AWS::ECS::Cluster |
| Cognito User Pools | JSON | CreateUserPool, AdminCreateUser, InitiateAuth (10 ops) | AWS::Cognito::UserPool |
| Cognito Identity | JSON | CreateIdentityPool, GetCredentialsForIdentity (4 ops) | AWS::Cognito::IdentityPool |
| Kinesis Data Streams | JSON | CreateStream, PutRecord, PutRecords, GetRecords (8 ops) | AWS::Kinesis::Stream |
| CloudFront | REST/XML | CreateDistribution, GetDistribution, UpdateDistribution (6 ops) | AWS::CloudFront::Distribution |
| RDS | Query | CreateDBInstance, CreateDBSnapshot, ModifyDBInstance (8 ops) | AWS::RDS::DBInstance |
| ElastiCache | Query | CreateCacheCluster, CreateReplicationGroup (6 ops) | AWS::ElastiCache::CacheCluster |
| EFS | REST/JSON | CreateFileSystem, CreateMountTarget, CreateAccessPoint (6 ops) | AWS::EFS::FileSystem |
| Glue | JSON | CreateDatabase, CreateTable, CreateJob, StartJobRun (10 ops) | AWS::Glue::Database |
| Cost Explorer | JSON | GetCostAndUsage, GetCostForecast | — |
| Budgets | JSON | CreateBudget, DescribeBudget, UpdateBudget, DeleteBudget (6 ops) | AWS::Budgets::Budget |
| Health | JSON | DescribeEvents, DescribeEventDetails (stub) | — |
| Organizations | JSON | CreateOrganization, DescribeOrganization, CreateAccount (6 ops) | — |
| SES v2 | REST/JSON | CreateEmailIdentity, SendEmail, GetEmailIdentity, ListEmailIdentities (5 ops) | AWS::SES::EmailIdentity |
| Kinesis Data Firehose | JSON | CreateDeliveryStream, PutRecord, PutRecordBatch, ListDeliveryStreams (6 ops) | AWS::KinesisFirehose::DeliveryStream |

### Known limitations

- **Cross-service IAM enforcement**: IAM policies are evaluated for IAM and STS operations.
  Per-operation enforcement for other services is planned.
- **Persistence**: In-memory by default; SQLite persistence available via `EventStoreConfig{Backend: "sqlite"}`.
- **Authentication**: SigV4 verification is opt-in (disabled by default for ease of testing).
  Enable via `ServerOptions.VerifySignatures = true`.
- **Partial operation coverage**: Each service emulates the most common operations.
  See [docs/services.md](docs/services.md) for the full operation list.

---

## Status

| Milestone | Status |
|-----------|--------|
| v0.1.0 — v0.9.0 | Complete |
| v0.10.0 — Lambda, SQS, S3 notifications | Complete |
| v0.11.0 — DynamoDB | Complete |
| v0.13.0 — EC2/VPC, fault injection, multi-region | Complete |
| v0.15.0 — ELB v2, Route 53 | Complete |
| v0.17.0 — Observability (metrics, tracing) | Complete |
| v0.18.0 — CloudWatch Logs, EventBridge, CloudWatch Alarms | Complete |
| v0.19.0–v0.23.0 — ACM, API Gateway, Step Functions, ECS, ECR, Cognito, Kinesis, CloudFront | Complete |
| v0.25.0–v0.26.0 — RDS, ElastiCache, EFS, Glue | Complete |
| v0.27.0 — Cost Explorer, Budgets, Health, Organizations | Complete |
| [v0.28.0](https://github.com/scttfrdmn/substrate/milestone/28) — SES v2, Firehose, Documentation | In Progress |

See [CHANGELOG.md](CHANGELOG.md) for full release history.

## Documentation

- [Getting Started](docs/getting-started.md) — install, first test, 15-minute tutorial
- [Service Reference](docs/services.md) — all 37 plugins with operation lists
- [Testing Guide](docs/testing-guide.md) — `StartTestServer`, recording/replay, cost assertions
- [Endpoint Configuration](docs/endpoint-configuration.md) — SDK and tool configuration

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
