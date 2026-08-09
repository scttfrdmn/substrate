# Testing Guide

This guide covers Substrate's testing APIs for Go developers. It assumes you
have already read [Getting Started](getting-started.md).

## Why Substrate Instead of Mocks

The alternative to an emulator is mocking individual AWS SDK interfaces. That
approach has a well-known failure mode: mock-based tests verify that your code
calls the right methods with the right arguments — they do not verify that the
behavior of those calls is correct.

In practice this means:

- A mock `PutObject` that returns `nil` tells you the call was made. It does
  not tell you whether the object is readable, whether it triggers an S3
  notification, or whether a subsequent `HeadObject` agrees on its size.
- Mock files grow large and brittle. A codebase that integrates S3, EC2, and
  IAM can accumulate 500+ lines of mock setup that must be kept in sync with
  the production code by hand.
- When the real AWS service behavior changes (new fields, different error codes,
  altered pagination), mocks silently diverge. The tests still pass; production
  still breaks.

Substrate tests are slower to write the first time, but the tests are
trustworthy in a way mocks cannot be: a `CreateBucket` call creates a bucket
that a subsequent `ListBuckets` will return, errors use real AWS error codes,
and cross-service dispatch (S3 → Lambda notifications, SQS → Lambda triggers)
works without hand-written stubs. The test exercises the code path, not a
description of it.

## Quick Start

`StartTestServer` is the entry point for all Go integration tests. It starts
an in-process Substrate server on a random port, registers all 63 service
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

The `TestServer` type exposes two fields, time and state helpers, and accessors
for the underlying components:

```go
type TestServer struct {
    URL  string // base URL, e.g. "http://127.0.0.1:54321"
    Port int    // TCP port
}

// State and time helpers
func (ts *TestServer) ResetState(t *testing.T)          // wipes all server state
func (ts *TestServer) AdvanceTime(d time.Duration)      // move the simulated clock forward
func (ts *TestServer) SetTime(t time.Time)              // set the simulated clock
func (ts *TestServer) SetScale(scale float64)           // set the time-acceleration factor
func (ts *TestServer) SeedSSMParameter(name, value string)
func (ts *TestServer) SeedSSMParameters(params map[string]string)

// Accessors for the underlying components
func (ts *TestServer) Store() *EventStore               // for cost summaries and recording/replay
func (ts *TestServer) StateManager() StateManager
func (ts *TestServer) TimeController() *TimeController
func (ts *TestServer) Registry() *PluginRegistry
```

`StartTestServer` returns when the `/health` endpoint responds — the server is
ready for requests immediately. The event store is enabled, so cost summaries
and recording/replay work against `ts.Store()` out of the box.

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

`StartTestServer` enables the event store and exposes the server's components
through accessors, so you build a `ReplayEngine` over the **same** store, state,
time controller, and registry the server is using — otherwise the engine would
record from a store the server never writes to:

```go
import "log/slog"

func setupTestHarness(t *testing.T) (*substrate.TestServer, *substrate.ReplayEngine) {
    t.Helper()

    ts := substrate.StartTestServer(t)

    engine := substrate.NewReplayEngine(
        ts.Store(),          // same EventStore the server records to
        ts.StateManager(),
        ts.TimeController(),
        ts.Registry(),
        substrate.ReplayConfig{
            RandomSeed:  42,
            StopOnError: true,
        },
        substrate.NewDefaultLogger(slog.LevelError, false),
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

    // Assert costs stay within $0.01. Read the summary from the SAME store the
    // server recorded to — ts.Store() — not a freshly constructed one, or the
    // summary will be empty. The account ID is the one the built-in test
    // credentials map to (123456789012).
    summary, err := ts.Store().GetCostSummary(ctx, "123456789012", time.Time{}, time.Time{})
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
summary, _ := ts.Store().GetCostSummary(ctx, accountID, start, end)
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
        substrate.ServerOptions{Fault: fc},
    )

    // Start and test...
    _ = srv
}
```

`FaultRule` fields:

| Field | Description |
|-------|-------------|
| `Service` | AWS service name (`"s3"`, `"lambda"`, …). Empty = all services. |
| `Operation` | Operation name (`"PutObject"`, …). Empty = all operations. Always the semantic operation, S3 included. |
| `PathSuffix` | Request path must end with this (`".parquet"`, `"/big.bin"`). Empty = any path. |
| `QueryKey` | Request must carry this query parameter (`"uploads"`, `"uploadId"`, `"partNumber"`). The value is not compared. Empty = any request. |
| `HeaderPrefix` | Request must carry a header whose name starts with this, compared case-insensitively. Empty = any request. |
| `FaultType` | `"error"` or `"latency"`. |
| `ErrorCode` | AWS error code returned on `"error"` faults. |
| `HTTPStatus` | HTTP status code (default 500). |
| `ErrorMsg` | Human-readable error message. |
| `LatencyMs` | Artificial delay in milliseconds for `"latency"` faults. |
| `Probability` | Fraction of matching requests that fire [0.0, 1.0]. |
| `Times` | How many matching requests the rule fires on. **Zero means one**, not unlimited; a negative value is unlimited. |
| `Fired` | Read-only: how many faults this rule has injected. Reported by `GET /v1/fault/rules` and summed by `FaultsFired()`. |

Rules are evaluated in order; the first matching rule that has not reached its
`Times` bound fires. Every non-empty matcher must hold, so a rule naming both an
operation and a `QueryKey` fires only where both apply.

Four things are worth knowing before writing a fixture (all four are #480):

- **`Operation` is the semantic name for S3 too.** A rule naming `PutObject` fires
  on `PutObject` and not on `UploadPart`, which is also a `PUT`. A rule naming a
  bare HTTP method no longer matches an S3 request at all.
- **`Times` is what makes retry assertable.** Fail twice, then succeed, is the
  outcome that distinguishes working retry from no retry; an unbounded rule can
  only ever produce failure. Set `Times: -1` when a fixture deliberately wants the
  old unbounded behavior — for instance when the test itself clears the rule and
  asserts the retry succeeds, which passes vacuously against a spent rule.
- **Assert `Fired`.** A rule that matches nothing produces exactly the same passing
  test as a consumer's retry working. `FaultsFired()` in process, or the per-rule
  `fired` member of `GET /v1/fault/rules` over the wire, is what tells them apart.
  Arming rules again resets the counts, so a fixture that re-arms between phases
  gets its full budget back.
- **`Probability` draws from a per-rule PRNG.** Each rule has its own stream, so its
  outcome sequence depends only on how many requests *that* rule matched — adding an
  unrelated rule, or a retry that another rule handles, does not shift it. Streams
  are keyed by a rule's index and reset when a config is armed, so reordering rules
  does change their outcomes. Prefer `Times` for a bounded outcome regardless: it
  needs no roll at all.

An injected error is serialized in the same wire shape the target service's own
errors use, so an SDK recovers the code rather than falling back to the HTTP status
(an injected S3 `SlowDown` used to arrive as `ServiceUnavailable`).

### Arming rules over the wire

A server started with `substrate server` always has a fault controller, whether or
not the config file has a `fault:` block, so a harness can arm rules through
`POST /v1/fault/rules` without restarting anything. A controller with `enabled:
false` makes injection a no-op until a rule is armed; only a `Server` constructed
in-process with no controller at all answers `501`.

```yaml
fault:
  enabled: false
  # Seeds the per-rule PRNGs behind `probability`. Default 0 — deterministic, so
  # the same config produces the same run every time. Set it to vary
  # probabilistic outcomes between runs deliberately.
  seed: 0
```

Rules armed over the wire replace the configured set and reset each rule's `fired`
count and PRNG stream, so a fixture that re-arms between phases starts each phase
from the same place.

## Testing IAM Permissions

Substrate evaluates IAM policies, so "the policy is too narrow" and "we forgot a
permission" are failures a test can catch instead of a real deployment finding them.
The class is worth naming: a worker launched with an instance profile granting no SQS
permissions passes every mocked test, and then never drains a queue.

Create a principal, attach a scoped policy, mint an access key, and call as it:

```bash
aws iam create-user --user-name alice
aws iam put-user-policy --user-name alice --policy-name ro --policy-document \
  '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}]}'
aws iam create-access-key --user-name alice   # returns the ID and secret

AWS_ACCESS_KEY_ID=<id> AWS_SECRET_ACCESS_KEY=<secret> aws sqs list-queues
# AccessDeniedException: User: arn:aws:iam::123456789012:user/alice is not
# authorized to perform: sqs:ListQueues on resource: *

AWS_ACCESS_KEY_ID=<id> AWS_SECRET_ACCESS_KEY=<secret> aws s3api list-objects --bucket b
# 200 — s3:ListBucket is what a listing authorizes against
```

STS session credentials work the same way. `AssumeRole` mints an `ASIA` key whose
calls are evaluated against the **role**, as real IAM resolves a session, so
`assume-role` then call is how a test exercises a role's policy.

### The assumption itself is gated, by the role's trust policy

Two policies decide an `AssumeRole` call, and they answer different questions. The
caller's own policies answer "what may this caller do" — that is the check above,
and it is why `alice` needs `sts:AssumeRole`. The role's **trust policy**
(`AssumeRolePolicyDocument`) answers "who may become this role", and it is a
*resource* policy: it names principals.

Both must hold, so a test can pin the confused-deputy defense — a role shared with
a third party that only admits a caller presenting a shared secret:

```bash
aws iam create-role --role-name broker --assume-role-policy-document \
  '{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
    "Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole",
    "Condition":{"StringEquals":{"sts:ExternalId":"secret-123"}}}]}'

# as alice, who is allowed to call sts:AssumeRole:
aws sts assume-role --role-arn arn:aws:iam::123456789012:role/broker \
  --role-session-name s1
# AccessDenied: User: arn:aws:iam::123456789012:user/alice is not authorized to
# perform: sts:AssumeRole because no role trust policy allows the
# sts:AssumeRole action

aws sts assume-role --role-arn arn:aws:iam::123456789012:role/broker \
  --role-session-name s1 --external-id secret-123
# 200 — an ASIA session key
```

The code is `AccessDenied`, where the identity-policy denial above reports
`AccessDeniedException`; both match what AWS returns. An explicit `Deny` in the
trust policy is reported with a different message ("with an explicit deny in the
role trust policy") under the same code.

**Writing a trust policy is the opt-in**, the same shape as the rule below one
level down. A role created without one is not enforced, so every test that never
wrote a trust policy keeps working — see
[the STS service reference](services.md#a-role-s-trust-policy-is-enforced-and-sts-externalid-with-it)
for the `Principal` forms that match.

### A principal that does not exist is not enforced

This is the one thing to know before writing such a test. **Existence in state is
the opt-in**: enforcement runs only when the access key resolves to an IAM user or
role substrate actually holds. An unregistered key, a key whose user was deleted, and
the credentials this guide uses elsewhere (`test`/`test`, `AKIAIOSFODNN7EXAMPLE`) all
resolve to no principal and are authorized against nothing.

There is no configuration flag. A test that never touches IAM is unaffected, and a
test that asserts a denial must create the principal first — a `put-user-policy`
against a user that was never created denies nothing, because there is no user to
evaluate.

Two consequences follow from the same rule. A principal that exists with **no**
policy is **denied** (an implicit deny, as on AWS), so a freshly created role with
nothing attached is the "forgot to attach the policy" case rather than a pass. And a
role named by a session but absent from state is not enforced, so `assume-role`
against a role substrate does not hold proves nothing.

In-process `emulator.Client` callers never sign a request, so they carry no principal
and are never authorized. Enforcement is reached over the wire. A trust policy is
skipped for the same caller and the same reason: with no principal there is nothing
for its `Principal` element to be true of.

### CloudFormation stacks

A stack's resource calls are authorized too, as the stack's service role when it has
one and as the creating principal otherwise. A refused resource call rolls the stack
back and names the denial in `DescribeStackResources`' `ResourceStatusReason` — not
as a `StackEvent`. See the CloudFormation section of
[the service reference](services.md) for the `RoleARN` semantics.

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
    substrate "github.com/scttfrdmn/substrate/emulator"
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
