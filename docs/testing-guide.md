# Testing Guide

This guide covers Substrate's testing APIs for Go developers. It assumes you
have already read [Getting Started](getting-started.md).

## Quick Start

`StartTestServer` is the entry point for all Go integration tests. It starts
an in-process Substrate server on a random port, registers all 37 service
plugins, and schedules `t.Cleanup` to shut the server down automatically.

```go
func TestMyService(t *testing.T) {
    ts := substrate.StartTestServer(t)
    // ts.URL is something like "http://127.0.0.1:54321"

    cfg, err := config.LoadDefaultConfig(context.Background(),
        config.WithRegion("us-east-1"),
        config.WithBaseEndpoint(ts.URL),
        config.WithCredentialsProvider(
            credentials.NewStaticCredentialsProvider("test", "test", ""),
        ),
    )
    if err != nil {
        t.Fatal(err)
    }

    // Use cfg with any AWS SDK v2 client.
    s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
        o.UsePathStyle = true
    })
    _ = s3Client
}
```

The `TestServer` type has two fields and one method:

```go
type TestServer struct {
    URL  string // base URL, e.g. "http://127.0.0.1:54321"
    Port int    // TCP port
}

func (ts *TestServer) ResetState(t *testing.T) // wipes all server state
```

`StartTestServer` returns when the `/health` endpoint responds — the server is
ready for requests immediately.

## State Isolation

Each call to `StartTestServer` creates a completely independent server with its
own in-memory state. For a single test function with multiple subtests, share
one server and call `ts.ResetState(t)` between subtests:

```go
func TestDynamoDBOperations(t *testing.T) {
    ts := substrate.StartTestServer(t)

    cfg, _ := config.LoadDefaultConfig(context.Background(),
        config.WithRegion("us-east-1"),
        config.WithBaseEndpoint(ts.URL),
        config.WithCredentialsProvider(
            credentials.NewStaticCredentialsProvider("test", "test", ""),
        ),
    )
    ddb := dynamodb.NewFromConfig(cfg)

    t.Run("CreateTable", func(t *testing.T) {
        defer ts.ResetState(t) // clean up after this subtest
        _, err := ddb.CreateTable(context.Background(), &dynamodb.CreateTableInput{
            TableName:   aws.String("users"),
            BillingMode: types.BillingModePayPerRequest,
            AttributeDefinitions: []types.AttributeDefinition{
                {AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
            },
            KeySchema: []types.KeySchemaElement{
                {AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
            },
        })
        if err != nil {
            t.Fatal(err)
        }
    })

    t.Run("ListTables_empty", func(t *testing.T) {
        defer ts.ResetState(t) // table from previous subtest is gone
        out, err := ddb.ListTables(context.Background(), &dynamodb.ListTablesInput{})
        if err != nil {
            t.Fatal(err)
        }
        if len(out.TableNames) != 0 {
            t.Fatalf("expected 0 tables, got %d", len(out.TableNames))
        }
    })
}
```

`ResetState` sends `POST /v1/state/reset` to the server and waits for 200 OK.
It calls `t.Fatal` if the reset fails, so you do not need to check the error.

You can also reset state from the CLI or another test language:

```bash
curl -X POST http://localhost:4566/v1/state/reset
# {"status":"ok"}
```

## Recording and Replay

Recording captures every AWS request as an immutable event stream. Replay
re-executes that stream through the same plugin registry for deterministic
CI reproduction.

### Wire up the ReplayEngine

`StartTestServer` does not expose the `ReplayEngine` directly — you construct
one and pass it the same store as the running server. The simplest approach is
to build your own test harness:

```go
func setupTestHarness(t *testing.T) (*substrate.TestServer, *substrate.ReplayEngine) {
    t.Helper()

    ts := substrate.StartTestServer(t)

    cfg := substrate.DefaultConfig()
    cfg.EventStore.Enabled = true // enable event store for recording

    store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
    state := substrate.NewMemoryStateManager()
    tc := substrate.NewTimeController(time.Now())
    registry := substrate.NewPluginRegistry()
    logger := substrate.NewDefaultLogger(slog.LevelError, false)

    ctx := context.Background()
    if err := substrate.RegisterDefaultPlugins(ctx, registry, state, tc, logger, store); err != nil {
        t.Fatalf("register plugins: %v", err)
    }

    engine := substrate.NewReplayEngine(store, state, tc, registry,
        substrate.ReplayConfig{
            RandomSeed:  42,
            StopOnError: true,
        },
        logger,
    )

    return ts, engine
}
```

### Record a session

```go
func TestRecordAndReplay(t *testing.T) {
    ts, engine := setupTestHarness(t)
    ctx := context.Background()

    // Start recording.
    session, err := engine.StartRecording(ctx, "my-infra-test")
    if err != nil {
        t.Fatal(err)
    }

    // Run operations against the server.
    cfg, _ := config.LoadDefaultConfig(ctx,
        config.WithRegion("us-east-1"),
        config.WithBaseEndpoint(ts.URL),
        config.WithCredentialsProvider(
            credentials.NewStaticCredentialsProvider("test", "test", ""),
        ),
    )
    s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
    _, _ = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("my-bucket")})

    // Stop recording.
    eventCount, err := engine.StopRecording(ctx, session)
    if err != nil {
        t.Fatal(err)
    }
    t.Logf("recorded %d events in stream %s", eventCount, session.StreamID)

    // Replay the session deterministically.
    results, err := engine.Replay(ctx, session.StreamID)
    if err != nil {
        t.Fatal(err)
    }
    if results.FailedEvents > 0 {
        t.Errorf("replay had %d failed events (expected 0)", results.FailedEvents)
    }
    t.Logf("replay: total=%d success=%d failed=%d duration=%s",
        results.TotalEvents, results.SuccessEvents,
        results.FailedEvents, results.Duration)
}
```

`RecordingSession` has two fields:

```go
type RecordingSession struct {
    StreamID  string        // event stream ID for later replay
    StartTime time.Time     // when recording began
}
```

`ReplayResults` summarises the run:

```go
type ReplayResults struct {
    TotalEvents   int
    SuccessEvents int
    FailedEvents  int
    SkippedEvents int
    Duration      time.Duration
    Differences   []*EventDifference  // response divergences
    StateValid    bool
    StateErrors   []string
}
```

<!-- TODO(#178): add section on persisting streams to SQLite for cross-run replay -->

## Time-Travel Debugging

When a replay is in progress you can jump to any event, step backward, and
inspect the full service state at that point.

```go
// Start a replay and pause at event 10.
results, err := engine.Replay(ctx, session.StreamID)

// Jump to event at sequence 87 (requires StopOnError or manual pause).
if err := engine.JumpToEvent(ctx, 87); err != nil {
    t.Fatal(err)
}

// Step backward one event.
prevEvent, err := engine.StepBackward(ctx)
if err != nil {
    t.Fatal(err)
}
t.Logf("stepped back to event: %s %s/%s",
    prevEvent.ID, prevEvent.Service, prevEvent.Operation)

// Inspect all S3 state at the current position.
s3State, err := engine.InspectState(ctx, "s3")
if err != nil {
    t.Fatal(err)
}
for key, val := range s3State {
    t.Logf("  s3/%s = %s", key, val)
}
```

`InspectState(ctx, namespace)` returns `map[string][]byte` — all key-value
pairs under the given namespace at the current replay position. Common
namespaces match service names: `"s3"`, `"dynamodb"`, `"lambda"`, `"iam"`, etc.

<!-- TODO(#178): document SetBreakpoint API once exposed -->

## Cost Assertions

Substrate tracks real AWS pricing per operation. Use `EventStore.GetCostSummary`
to assert that an operation sequence stays within budget.

```go
func TestCostBudget(t *testing.T) {
    ts := substrate.StartTestServer(t)
    ctx := context.Background()

    cfg, _ := config.LoadDefaultConfig(ctx,
        config.WithRegion("us-east-1"),
        config.WithBaseEndpoint(ts.URL),
        config.WithCredentialsProvider(
            credentials.NewStaticCredentialsProvider("test", "test", ""),
        ),
    )

    // Run a workload: create table, write 1000 items, scan.
    ddb := dynamodb.NewFromConfig(cfg)
    // ... create table and items ...

    // Assert costs stay within $0.01.
    store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true})
    summary, err := store.GetCostSummary(ctx, "000000000000", time.Time{}, time.Time{})
    if err != nil {
        t.Fatal(err)
    }

    const maxCost = 0.01
    if summary.TotalCost > maxCost {
        t.Errorf("cost $%.6f exceeds budget $%.6f", summary.TotalCost, maxCost)
        for svc, cost := range summary.ByService {
            t.Logf("  %s: $%.6f", svc, cost)
        }
    }
}
```

`CostSummary` fields:

```go
type CostSummary struct {
    AccountID    string
    TotalCost    float64             // USD
    ByService    map[string]float64  // service name → USD
    ByOperation  map[string]float64  // "service/operation" → USD
    RequestCount int64
    StartTime    time.Time
    EndTime      time.Time
}
```

Pass a non-zero `start` and `end` to restrict the summary to a time window:

```go
start := time.Now().Add(-1 * time.Hour)
end := time.Now()
summary, _ := store.GetCostSummary(ctx, accountID, start, end)
```

## Fault Injection

`FaultController` injects configurable errors and latency into the request
pipeline. Use it to test retry logic, circuit breakers, and error handling paths.

```go
func TestRetryOnS3Error(t *testing.T) {
    // Create a FaultController that returns InternalError for 50% of S3 PutObject calls.
    fc := substrate.NewFaultController(substrate.FaultConfig{
        Enabled: true,
        Rules: []substrate.FaultRule{
            {
                Service:     "s3",
                Operation:   "PutObject",
                FaultType:   "error",
                ErrorCode:   "InternalError",
                HTTPStatus:  500,
                ErrorMsg:    "injected fault",
                Probability: 0.5,
            },
        },
    }, 42 /* seed — fixed for determinism */)

    // Wire the FaultController into a server manually (for fault injection
    // you construct the server directly rather than using StartTestServer).
    cfg := substrate.DefaultConfig()
    cfg.Server.Address = "127.0.0.1:0"

    state := substrate.NewMemoryStateManager()
    tc := substrate.NewTimeController(time.Now())
    registry := substrate.NewPluginRegistry()
    logger := substrate.NewDefaultLogger(slog.LevelError, false)
    store := substrate.NewEventStore(substrate.EventStoreConfig{})

    ctx := context.Background()
    _ = substrate.RegisterDefaultPlugins(ctx, registry, state, tc, logger, store)

    srv := substrate.NewServer(*cfg, registry, store, state, tc, logger,
        substrate.WithFaultController(fc),
    )

    // Start and test...
    _ = srv
}
```

`FaultRule` fields:

| Field | Description |
|-------|-------------|
| `Service` | AWS service name (`"s3"`, `"lambda"`, …). Empty = all services. |
| `Operation` | Operation name (`"PutObject"`, …). Empty = all operations. |
| `FaultType` | `"error"` or `"latency"`. |
| `ErrorCode` | AWS error code returned on `"error"` faults. |
| `HTTPStatus` | HTTP status code (default 500). |
| `ErrorMsg` | Human-readable error message. |
| `LatencyMs` | Artificial delay in milliseconds for `"latency"` faults. |
| `Probability` | Fraction of matching requests that fire [0.0, 1.0]. |

Rules are evaluated in order; the first matching rule fires.

<!-- TODO(#178): document server.WithFaultController option once stabilised -->

## Multi-Region

Substrate supports multiple regions. Resources are scoped by `(account, region)`
in state. To test multi-region workloads, create multiple SDK clients pointing
at the same Substrate server with different regions:

```go
func TestMultiRegion(t *testing.T) {
    ts := substrate.StartTestServer(t)

    makeClient := func(region string) *s3.Client {
        cfg, _ := config.LoadDefaultConfig(context.Background(),
            config.WithRegion(region),
            config.WithBaseEndpoint(ts.URL),
            config.WithCredentialsProvider(
                credentials.NewStaticCredentialsProvider("test", "test", ""),
            ),
        )
        return s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
    }

    usEast := makeClient("us-east-1")
    euWest := makeClient("eu-west-1")

    ctx := context.Background()

    // Buckets in different regions are independent.
    _, err := usEast.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("east-bucket")})
    if err != nil {
        t.Fatal(err)
    }

    _, err = euWest.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("west-bucket")})
    if err != nil {
        t.Fatal(err)
    }

    // ListBuckets is global — shows all buckets regardless of region.
    out, _ := usEast.ListBuckets(ctx, &s3.ListBucketsInput{})
    t.Logf("total buckets: %d", len(out.Buckets))
}
```

Most services scope state by `(account, region)`. A few are global:
CloudFront (always `us-east-1`), Route 53, IAM, STS, and Organizations.

<!-- TODO(#178): add guidance on testing cross-region replication patterns -->

## Full Example: Lambda + SQS end-to-end

This example creates an SQS queue, registers a Lambda trigger, sends a message,
and verifies the Lambda was invoked.

```go
package myapp_test

import (
    "context"
    "testing"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/credentials"
    awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
    substrate "github.com/scttfrdmn/substrate"
)

func TestLambdaSQSTrigger(t *testing.T) {
    ts := substrate.StartTestServer(t)
    ctx := context.Background()

    cfg, err := config.LoadDefaultConfig(ctx,
        config.WithRegion("us-east-1"),
        config.WithBaseEndpoint(ts.URL),
        config.WithCredentialsProvider(
            credentials.NewStaticCredentialsProvider("test", "test", ""),
        ),
    )
    if err != nil {
        t.Fatal(err)
    }

    sqsClient := sqs.NewFromConfig(cfg)
    lambdaClient := awslambda.NewFromConfig(cfg)

    // Create a queue.
    qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
        QueueName: aws.String("my-queue"),
    })
    if err != nil {
        t.Fatalf("CreateQueue: %v", err)
    }
    t.Logf("queue URL: %s", *qOut.QueueUrl)

    // Create a Lambda function stub.
    _, err = lambdaClient.CreateFunction(ctx, &awslambda.CreateFunctionInput{
        FunctionName: aws.String("my-processor"),
        Runtime:      "nodejs18.x",
        Role:         aws.String("arn:aws:iam::123456789012:role/exec"),
        Handler:      aws.String("index.handler"),
        Code: &awslambda.FunctionCode{
            ZipFile: []byte("stub"),
        },
    })
    if err != nil {
        t.Fatalf("CreateFunction: %v", err)
    }

    // Send a message.
    _, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
        QueueUrl:    qOut.QueueUrl,
        MessageBody: aws.String(`{"event": "test"}`),
    })
    if err != nil {
        t.Fatalf("SendMessage: %v", err)
    }

    // Receive the message (Substrate does not invoke Lambda on SQS receive —
    // that cross-service dispatch is approximated; see services.md for details).
    recv, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
        QueueUrl:            qOut.QueueUrl,
        MaxNumberOfMessages: 1,
    })
    if err != nil {
        t.Fatalf("ReceiveMessage: %v", err)
    }
    if len(recv.Messages) != 1 {
        t.Fatalf("expected 1 message, got %d", len(recv.Messages))
    }
    t.Logf("received: %s", *recv.Messages[0].Body)
}
```

<!-- TODO(#178): expand with full SNS→SQS→Lambda fan-out example -->
