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

## Status

| Milestone | Status |
|-----------|--------|
| [v0.1.0 — Event sourcing foundation](https://github.com/scttfrdmn/substrate/milestone/1) | In progress |
| [v0.2.0 — Core server and plugins](https://github.com/scttfrdmn/substrate/milestone/2) | Planned |
| [v0.3.0 — IAM implementation](https://github.com/scttfrdmn/substrate/milestone/3) | Planned |
| [v0.4.0 — Quotas, consistency, costs](https://github.com/scttfrdmn/substrate/milestone/4) | Planned |
| [v0.5.0 — S3 plugin](https://github.com/scttfrdmn/substrate/milestone/5) | Planned |
| [v1.0.0 — Production release](https://github.com/scttfrdmn/substrate/milestone/7) | Planned |

## Quick Start

```bash
go get github.com/scttfrdmn/substrate
```

```go
import "github.com/scttfrdmn/substrate"

store  := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
tc     := substrate.NewTimeController(time.Now())
engine := substrate.NewReplayEngine(store, nil, tc, substrate.NewPluginRegistry(),
    substrate.ReplayConfig{}, logger)

session, _ := engine.StartRecording(ctx, "my-test")
// ... run tests against AWS SDK at localhost:4566 ...
engine.StopRecording(ctx, session)

results, _ := engine.Replay(ctx, session.StreamID)
fmt.Printf("replayed %d events, %d differences\n",
    results.TotalEvents, len(results.Differences))
```

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
