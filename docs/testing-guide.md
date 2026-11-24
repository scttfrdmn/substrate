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
an in-process Substrate server on a random port, registers every service
plugin, and schedules `t.Cleanup` to shut the server down automatically.

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
func (ts *TestServer) ResetState(tb testing.TB)         // wipes all server state
func (ts *TestServer) AdvanceTime(d time.Duration)      // move the simulated clock forward
func (ts *TestServer) SetTime(t time.Time)              // set the simulated clock
func (ts *TestServer) SetScale(scale float64)           // set the time-acceleration factor
func (ts *TestServer) FreezeTimeAt(t time.Time)         // stop the simulated clock at exactly t
func (ts *TestServer) FreezeTime()                      // stop it where it stands
func (ts *TestServer) UnfreezeTime()                    // resume from where it was stopped
func (ts *TestServer) TimeFrozen() bool
func (ts *TestServer) SeedSSMParameter(name, value string)
func (ts *TestServer) SeedSSMParameters(params map[string]string)

// Accessors for the underlying components
func (ts *TestServer) Store() *EventStore               // for cost summaries and recording/replay
func (ts *TestServer) StateManager() StateManager
func (ts *TestServer) TimeController() *TimeController
func (ts *TestServer) Registry() *PluginRegistry
func (ts *TestServer) RequestIDs() *RequestIDSource     // the seeded request-ID sequence
```

`StartTestServer` returns when the `/health` endpoint responds — the server is
ready for requests immediately. The event store is enabled, so cost summaries
and recording/replay work against `ts.Store()` out of the box.

`SetTime` sets where the clock *starts from*, not what it reads: the clock then
advances with wall time at its scale, so a value rendered from it — a
`CreationDate`, a `CreatedTime` — depends on how fast the machine ran. At the
second resolution most AWS timestamps are rendered at, that is usually invisible
and occasionally a one-second difference. Use **`FreezeTimeAt`** when a test
asserts an exact timestamp; a frozen clock reads the same instant however long the
test takes, and `AdvanceTime` still moves it by a known interval. Prefer
`FreezeTimeAt(t)` over `SetTime(t)` followed by `FreezeTime()` — the latter stops
the clock a few tens of nanoseconds after `t`, which is enough to cross a second
boundary if `t` sits near one.

## Deterministic Runs

Every identifier a request mints — a VPC ID, an instance ID, a KMS key ID, an IAM user
ID, an S3 `UploadId`, an Organizations account ID — is derived from that request's ID. A
replay restores the recorded request IDs, so a replay mints what the recording minted.
**Deterministic-run mode** makes two *fresh* runs agree as well: it seeds the request IDs
and pins the clock, so the same calls in the same order answer byte-identical bodies
(#1598).

- **Seeded request IDs.** A seeded server assigns `req-<8 hex>-<n>`: the prefix is derived
  from the seed and `n` counts the requests the server has served. Every top-level
  request draws one, in arrival order, a refused one included.
- **A pinned clock.** The clock is frozen at an epoch, so every timestamp a response
  renders is that instant until the test moves it.
- **Reset.** `POST /v1/state/reset` (and `ts.ResetState`) rewinds the sequence and returns
  a pinned clock to its epoch, so each test case sharing a server sees what a fresh
  server would give it.

`StartTestServer` seeds request IDs by default, so identifiers already repeat from run to
run. The options:

```go
ts := substrate.StartTestServer(t,
    substrate.WithDeterministicRun(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)))
// Request IDs seeded, clock frozen at 2026-01-01T00:00:00Z.

ts := substrate.StartTestServer(t, substrate.WithoutSeededIDs())
// Wall-clock, random request IDs — unique across servers.
```

The same settings for the `substrate` binary:

| Setting | Config key | Environment | Flag |
|---------|-----------|-------------|------|
| Request-ID seed (0 is unseeded, the default) | `run.seed` | `SUBSTRATE_RUN_SEED` | `--seed` |
| Pinned epoch (RFC 3339; empty leaves the clock running) | `run.epoch` | `SUBSTRATE_RUN_EPOCH` | `--epoch` |

```yaml
run:
  seed: 42
  epoch: "2026-01-01T00:00:00Z"
```

Over HTTP, `GET /v1/control/run` reports `seed`, `epoch`, `frozen` and `drawn`. `epoch` is
present only while the clock is pinned, and it is the instant a reset returns the clock to.
`drawn` is the number of request IDs assigned since the seed was set or the state last reset.
`POST /v1/control/run` with `{"seed": 42}` reseeds and restarts the sequence (0 unseeds);
`{"epoch": "2026-01-01T00:00:00Z"}` pins the clock and `{"epoch": ""}` unpins it. A field
left out is left as it is. `POST /v1/control/time` accepts `"frozen": true` or `false`
beside `"time"`, which is the HTTP form of `FreezeTimeAt` and `UnfreezeTime`. `"frozen": false`
(and `UnfreezeTime`) also unpins the clock, so a later reset leaves it running;
`"frozen": true` freezes without pinning. Neither endpoint is recorded as an event, so a
replay does not re-apply them.

An in-process `Client` draws from a server's sequence when built with
`substrate.WithClientRequestIDs(ts.RequestIDs())`.

**Timestamps a pinned clock refuses.** An operation that checks a caller's timestamp against
the clock checks it against the simulated one. CloudWatch's `PutMetricData` refuses a datum
`Timestamp` more than two weeks before the clock or more than two hours after it (#1595), so
under an epoch pinned in the past a client that stamps datapoints with its own wall clock is
refused with `InvalidParameterValue`. Stamp them relative to the server's clock, which a test
reads from `ts.TimeController().Now()`, or omit `Timestamp`, and the datapoint takes the
simulated clock.

**What stays nondeterministic.** These are documented limits rather than defects:

- **Concurrent requests.** Requests a client sends at once draw their IDs in whichever
  order the scheduler runs them, so the identifiers minted from them can swap between
  runs. Requests that do not overlap are assigned in the order they arrive.
- **The `Date` response header**, which the HTTP server writes from the wall clock.
- **Substrate's bookkeeping IDs**: event IDs, snapshot IDs, and replay and recording
  session IDs. No AWS call observes them.
- **Request IDs repeat after a reset.** The reset rewinds the sequence, so a server's event
  log can hold the same request ID twice: once before a reset and once after it. A replay
  of that stream re-runs both halves against one state, so a seeded stream replays
  faithfully only between resets (#1631 tracks recording the reset itself).
- **Unaudited map iteration.** A response whose order comes from iterating a Go map, in a
  plugin path nobody has checked, can differ between runs. The acceptance test covers EC2,
  KMS, IAM, S3 multipart uploads, Organizations and Firehose. Sixteen `test/e2e` journeys run
  under `RunTwice` (below), and the Terraform and CDK journeys under `RunThenReplay`, which
  extends that to what they call: EC2 VPCs, subnets, security groups, instances, volumes, snapshots and tags; IAM
  users, roles, groups, policies and tags; S3 buckets; SQS; SNS; DynamoDB; KMS; Lambda;
  CloudWatch and CloudWatch Logs; CloudFormation stacks and events; STS; Organizations;
  Config; Cognito; Secrets Manager; EventBridge; SSM parameters and ECR repositories (through
  CDK bootstrap); and WAFv2. Report any other path you find.

### Checking a journey twice

Two helpers check the coverage above (#1628). Which one a journey uses depends on its caller.

- **`emulator.RunTwice(t, epoch, journey)` is for an in-process client that issues a fixed
  sequence of calls**, such as the Go SDK journeys in `test/e2e`.
  - It runs `journey` against two fresh servers started with `WithDeterministicRun(epoch)`.
  - It pairs the two servers' recorded events by sequence and fails unless they agree: the
    same count and operations, and the same request IDs, statuses, errors, headers other than
    `Date`, and response bodies.
  - Bodies are compared as [replay](#what-a-replay-compares) compares them: JSON member order
    is ignored, and collection order is compared.
  - Each run is a subtest, `first` and `second`.
  - The journey must not send requests concurrently, read the wall clock, or depend on a server
    another test shares.
  - `emulator.CompareRecordedRuns(first, second)` makes the same comparison on two event logs
    you already hold.
- **`emulator.RunThenReplay(t, epoch, journey)` is for an external tool whose calls depend on
  timing**, such as Terraform and the CDK CLI. Both journeys in `test/iac` use it.
  - It runs `journey` once, then replays the recorded requests, in recorded order, into a second
    fresh server. It fails as `AssertReplayMatches` does.
  - A tool like this does not make the same calls on every run. Terraform walks independent
    resources in scheduler order. Both tools poll their waiters on the wall clock and look
    credentials up a varying number of times. One CDK run made a `GetCallerIdentity` call the
    other did not.
  - A positional comparison of two such runs reports the tool's timing, which is client-side
    nondeterminism of the same kind as the concurrent-request limit above. Replaying gives the
    second server the identical sequence.

```go
func TestCheckout(t *testing.T) {
    emulator.RunTwice(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
        func(t *testing.T, ts *emulator.TestServer) {
            // one SDK request at a time against ts.URL
        })
}
```

See [Terraform and CDK](/iac-journeys) for the IaC journeys.

`make nondeterminism-check` keeps plugin code honest. Outside a reviewed allowlist, it
refuses in emulator code: a read of the wall clock (`time.Now`, `time.Since`, `time.After`
and their relatives), a `math/rand` or `crypto/rand` import, and an unseeded identifier
(`generateRequestID`, `NewIDMint("")`).

### Benchmarks

Every harness entry point takes `testing.TB`, so a `*testing.B` can drive a real
server over HTTP rather than falling back to an in-memory fake:

```go
func BenchmarkUpload(b *testing.B) {
    ts := substrate.StartTestServer(b)
    defer ts.ResetState(b)

    for range b.N {
        // ... requests against ts.URL
    }
}
```

Do the setup before `b.ResetTimer()`, and keep `ResetState` out of the measured
loop unless the reset itself is what you are measuring — it is a round trip to
the server.

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
re-executes that stream for deterministic CI reproduction.

### What is recorded

Every request and response is recorded with its body by default
(`event_store.include_bodies: true`), so a stream can be replayed without any setup.
Three rules apply to what an event stores:

- **Credentials are never recorded.** The `Authorization`, `Proxy-Authorization`,
  `X-Amz-Security-Token`, `X-Amz-Date`, `X-Amz-Signature` and `X-Amz-Credential`
  headers are removed, and so are the `X-Amz-*` signature parameters of a presigned
  URL. A Signature Version 2 request, which carries `AWSAccessKeyId`, also loses
  `AWSAccessKeyId`, `Signature`, `SignatureMethod`, `SignatureVersion` and
  `SecurityToken`, from its parameters, its repeated parameters and its form-encoded
  body. Replay does not need them: it reads the parsed parameters, restores the caller
  from the recorded principal ARN, and does not verify signatures.
- **Bodies are capped.** A body larger than `event_store.max_body_bytes` (default
  1 MiB) is replaced by its length and SHA-256. An event whose *request* body was
  dropped cannot be re-executed, and replay counts it as skipped with the reason
  `request body over max_body_bytes`. A *response* body that was dropped is compared
  by digest, byte for byte: the JSON member-order and XML whitespace normalisation the
  body comparison applies does not apply to a digest. A negative value removes the cap.
- **Memory is not reclaimed.** A `memory` store keeps every event for the life of the
  process, so a long-running `substrate server` holds every recorded body. Use the
  `file` or `sqlite` backend to keep a stream across processes, which is also what
  `substrate replay` and `substrate debug` read.

`StartTestServer` records bodies too. `WithoutRecordedBodies()` turns that off, after
which every event replays as skipped. `WithRecordedBodies()` is kept so older tests
compile, and has no effect.

### Streams

Every request that arrives over HTTP is recorded in the stream named `"default"`
(`substrate.DefaultStreamID`). Nothing on the wire selects another stream. The
CloudFormation deployer records each stack's resource calls in a stream named after
the stack, and `Client.Deploy` uses a `deploy-<n>` stream. `ReplayEngine.StartRecording`
returns a new stream name, but no HTTP request is ever recorded in it ([#1630](https://github.com/scttfrdmn/substrate/issues/1630)), so replay
`"default"` instead.

A stack's resource calls are recorded in the stack's stream, and each names the request
it ran for (`Event.ParentRequestID`). A replay that also re-runs that `CreateStack` skips
them (reason `internal dispatch`), because the re-run dispatches them again. A replay of
the stack's own stream, which does not hold the `CreateStack`, executes them. A dispatch
recorded before parents were recorded, in a stream that holds a top-level request, is
skipped with the unverified reason `internal dispatch with no recorded parent`.

### Replay a test server's session

```go
func TestRecordAndReplay(t *testing.T) {
    ts := substrate.StartTestServer(t)

    // ... run the code under test against ts.URL ...

    // Replays the "default" stream into a fresh emulator built from the server's
    // configuration, and fails the test on any difference or unverified skip.
    results := ts.AssertReplay(t)
    t.Logf("replay: total=%d success=%d skipped=%d", results.TotalEvents,
        results.SuccessEvents, results.SkippedEvents)
}
```

`ts.Replay(t)` does the same without asserting, and returns the `*ReplayResults`.
Both replay into a `ReplaySandbox` (`ts.ReplaySandbox(t)`), with a control plane so
recorded seeds are re-applied, the quota, consistency and fault controllers the server
ran with, and state-hash validation when the server records hashes. The server's own state is not touched, so a test can keep
calling it afterwards. A `ts.ResetState(t)` call is not an event yet ([#1631](https://github.com/scttfrdmn/substrate/issues/1631)), so a stream that spans
one replays as one continuous run.

`substrate.AssertReplayMatches(t, results, allow...)` is the assertion on its own. It
fails for every difference whose field matches none of the `allow` patterns
(`path.Match` syntax, for example `"response_body/*/TableId"`), and for every event
skipped for a reason that leaves it unchecked: no recorded request, a request body over
the cap, a seed replayed without a control plane, or an internal dispatch with no
recorded parent. A clock-driven or internal dispatch skip is not a failure, because the
parent request reproduces it.
`substrate.ReplayDifferences(recorded, replayed)` compares two `*AWSResponse` values
the same way.

### Wire up the ReplayEngine yourself

To drive the engine directly against the server — replaying into the server's own
state, which the replay resets first — build it over the **same** store, state, time
controller, and registry the server is using:

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
            StopOnError: true,
        },
        substrate.NewDefaultLogger(slog.LevelError, false),
        // Lets a recorded seed be re-applied during the replay; see "A seeded
        // outcome replays under the same seed" below.
        substrate.WithControlPlaneHandler(ts.ControlPlaneHandler()),
    )

    return ts, engine
}
```

### Replay through the engine

```go
func TestReplayThroughTheEngine(t *testing.T) {
    ts, engine := setupTestHarness(t)
    ctx := context.Background()

    cfg, _ := config.LoadDefaultConfig(ctx,
        config.WithRegion("us-east-1"),
        config.WithBaseEndpoint(ts.URL),
        config.WithCredentialsProvider(
            credentials.NewStaticCredentialsProvider("test", "test", ""),
        ),
    )
    s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
    _, _ = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("my-bucket")})

    // HTTP requests are recorded in the "default" stream.
    results, err := engine.Replay(ctx, substrate.DefaultStreamID)
    if err != nil {
        t.Fatal(err)
    }
    substrate.AssertReplayMatches(t, results)
}
```

`ReplayResults` summarises the run:

```go
type ReplayResults struct {
    TotalEvents   int
    SuccessEvents int                         // re-executed without an error
    FailedEvents  int                         // returned an error
    SkippedEvents int                         // not re-executed
    SkipReasons   map[ReplaySkipReason]int    // SkippedEvents by reason
    Duration      time.Duration
    Differences   []*EventDifference          // divergences from the recording
    StateValid    bool
    StateErrors   []string
}
```

`ReplayEngine.ReplayEvents(ctx, events)` replays a slice of events that is not in the
engine's store, such as the ones a regression fixture embeds.

### What a replayed response reproduces

A replayed success body carries the `RequestId` the **recording** was served under, not
a freshly minted one. The event records it, and the replay dispatches the handler with
it, so the 66 places that render a request id into a response reproduce the recorded
bytes (#866).

A **live** request's id still derives from the wall clock, deliberately. What
determinism asks for is that a recorded run replays to the same bytes; deriving a live
id from the request instead would collide two identical requests onto one value, and a
caller correlating a response with a log line needs them distinct. An error body takes
the other route and carries the fixed id `SUBSTRATE`, because there the id identifies
the emulator rather than the request.

So two assertions are safe and one is not:

```go
// Safe: a replay of a recorded stream renders the recorded id.
// Safe: an error body's RequestId is always "SUBSTRATE".
// Not safe: two *live* calls with the same input share a RequestId. They do not.
```

A stream recorded before the event carried a request id replays with the event id in
that field, since the original value was never written down and nothing can recover it.

**A replayed timestamp is the recorded one, exactly.** The simulated clock is *frozen*
at the recorded event's own timestamp for the duration of that event's replay, so every
read of it while the handler runs returns that timestamp however long the replay
dispatch takes. Before #1217 the clock was set to the recorded timestamp and then left
advancing, which made a replayed date reproducible only to within that latency: at
second resolution it matched the recording except when the recorded and replayed reads
straddled a second boundary, and then the replay reported a one-second difference. So
`Differences` being empty is now an assertion a test can rely on for a body carrying a
timestamp, and a run exported as a regression fixture no longer carries a one-in-N
failure. The clock is restored afterwards, so replaying on a live emulator does not stop
its clock — unless it was already frozen, in which case it is left that way.

**…and the recorded timestamp is the instant the live request read, even on a running
clock.** Freezing the replay at the recorded timestamp is exact only if that timestamp is
the instant the live handler used. Until #1396 it was not: the event was stamped after the
handler ran, so on a running clock a date rendered at millisecond precision replayed a
millisecond or more late (`CreationTime` `…480` live, `…481` on replay). Each top-level
request now holds the clock for its duration (`TimeController.Hold`): every read its
handler, gates, nested dispatches and clock-driven work make, and its event's timestamp,
are one instant. A replay therefore renders every recorded date byte-identically, at any
resolution, on a running clock as on a frozen one.

The guarantee is for requests that do not overlap. A hold lasts as long as the request
that took it, and a request arriving during another's hold reads that instant until it is
released, then the running clock. So two *concurrent* requests can render dates a serial
replay cannot reproduce, which matches what a replay promises about concurrency
generally, since the interleaving is not recorded. A hold is not a freeze: the clock keeps
advancing underneath, so simulated time is not lost to request latency.

**A replayed create reproduces the identifiers it minted.** An identifier substrate mints
is derived from the request's own id, so the replayed `CreateVpc` answers with the
recording's `vpc-…` and the recorded `CreateSubnet` that names it succeeds. This is what
makes `Differences` empty — and `StateValid` true, with `WithRecordedStateHashes` and
`ReplayConfig{ValidateState: true}` — an assertion a stream containing creates can make at
all; before #856 every such stream diverged on its first create, which is why the #1140
tests above are built on caller-chosen bucket and key names instead.

**That now includes a CloudFormation stack.** A deploy turns each resource into an internal
request, and those requests used to carry no mint, so every physical resource ID in every
stack was random even though the deployer holds no draw site of its own — a replayed
`DescribeStackResources` reported different IDs, in a 200, and `CreateStack`'s
`state_hash_after` never matched. A stack of a handful of resources now replays with zero
differences and `StateValid` true.

Two caveats. **Not every service's identifiers are derived yet.** EC2, IAM, STS, SQS, SNS,
Lambda, EFS, FSx, Transfer, ECS, Step Functions, EventBridge, CloudWatch Logs, CloudFront,
Service Quotas, API Gateway (v1 and v2), AppSync, Batch, EMR Serverless, ECR, ELB, Route 53,
Cognito (both APIs), IAM Identity Center, KMS, ACM, Secrets Manager, WAFv2, Athena,
Redshift Data, Glue, Timestream, OpenSearch, QuickSight, CodeBuild, CodeDeploy,
CodePipeline, SSM Run Command, RAM, AWS Backup and Bedrock batch inference are; the rest are
migrating one family at a time, and until a service moves, a
replay of a stream creating one of its resources still diverges. And a recording made against an
**unfrozen** clock can still diverge on a `state_hash_after` even when every identifier
matches, because a handler reading the live clock stamps its record a few hundred
nanoseconds after the event's own timestamp — invisible in a response rendering seconds,
visible in a hash taken over a stored `time.Time`. Freeze the clock in any test that
asserts on state hashes, which is what "no test depends on wall-clock time" asks for
anyway.

### What a replay re-executes

A live request passes through nine pipeline steps before its response is written, and a
replay does not re-run all of them. Four are re-decided, four are skipped, and one is
replaced — each for a reason about what the recording settled and what it did not
(#833):

| Step | On replay |
|---|---|
| Authorization (`AuthController.CheckAccess`) | **re-decided**, against the principal the event records |
| Quota (`QuotaController.CheckQuota`) | **re-decided**, with the token buckets rewound and the clock at the recorded instant; exact for serialized traffic only |
| Consistency (`ConsistencyController.CheckRead`) | **re-decided**: replayed writes open their windows and replayed reads are checked against them; exact for serialized traffic only |
| Fault injection | **re-decided**, with the controller rewound to its armed state |
| Credential resolution | replaced by principal restoration — the recorded access key resolves to nothing |
| SigV4 verification | skipped: the canonical request cannot be rebuilt, and the signature was checked once already |
| Presigned-URL expiry | skipped: a statement about the wall-clock moment the URL was signed |
| Protocol conflict, region allow-list | skipped: properties of the HTTP request and of the replay's own config, not of the recorded run |

A refusal at any re-decided step stops the request before it reaches a plugin, exactly
as it did live, so the replay applies no state change the recording does not contain. It
is compared against the recorded error, counts in `FailedEvents` — which means
"returned an error", not "diverged" — and is reported as an `EventDifference` only when
it disagrees with the recording.

Authorization is re-decided from `Event.Principal`, which is the caller's **ARN**. That
is what makes it work at all: an access key is re-minted differently on replay, but
nothing in the authorization path reads the key, and an IAM user's or assumed role's ARN
is derived from names the recorded requests themselves carried. The one condition key a
replayed principal cannot publish is `aws:userid` — an `AIDA…` or `<role-id>:<session>`
is minted and unrecoverable from the ARN — so it is absent rather than wrong, which is
the same choice substrate makes live for a caller it has no ID for. A policy
conditioning on it replays as though the key were unset. An event with no principal (an
unsigned request, which is most of them) replays unenforced, exactly as it was served.

Quota and consistency are re-decided rather than exempted (#1601). An exempt replay
re-executed a throttled or inconsistent request and applied a change the recording
never made. Each replay rewinds both controllers, and the clock is frozen at each
event's recorded instant, so token refills and propagation windows reach the recorded
decisions. A replay reproduces them only when it is configured as the recording was: a
`ReplaySandbox` built by the server or `TestServer` copies the controllers the server
actually ran, and `substrate replay` and `substrate debug state-at` build them from the
`quotas:` and `consistency:` sections. A CloudFormation or API Gateway internal dispatch
passes authorization only, as it did live: neither the deployer nor the proxy consults
quota, consistency or faults.

**Quota and consistency replay exactly only for serialized traffic.** Live, overlapping
requests reach the two gates in whatever order they take the controller's lock, and a
replay decides them in recorded sequence order. Two requests that overlapped in time can
therefore swap a throttle, or a window, between them on replay. This is the same
exception deterministic-run mode documents for concurrent requests (#1598). A difference
on an event whose recorded timestamp and duration overlap another's is likely this.
An event a replay skips, recorded without its request or with a request body over the
cap, is still charged to both gates (a quota token, and a window for a write that
reached its plugin), so the decisions after it stay aligned.

Fault injection needs one thing from the caller. A rule's `times` bound and its
per-rule random stream are state a run *spends*, so the engine rewinds the controller to
the configuration it was armed with at the start of each replay; without that, a rule
that fired during the recording is spent and the replay of the request it failed takes
the unfaulted path. A latency rule's delay is reported to the replay and **not** slept:
the recorded `Duration` is what the live run took, and no replay may consume wall-clock
time. One consequence is worth stating: a probabilistic rule's draw sequence depends on
the requests it sees, so a stream that skipped events for want of `include_bodies`
shifts every later draw. Reproducing a probabilistic fault takes a fully-bodied stream.

In Go, the controllers reach the engine through one option, and a caller that supplies
none gets an ungated replay — the recorded refusal is then reported as a critical
difference:

```go
engine := substrate.NewReplayEngine(store, state, tc, registry, cfg, logger,
    substrate.WithReplayPipeline(substrate.ReplayPipeline{
        Auth:  ts.AuthController(),
        Fault: ts.FaultController(),
    }))
```

`substrate replay` builds all four itself: the `quotas:`, `consistency:` and `fault:`
sections `substrate server` reads, and an authorization controller over the same state
manager the replay rebuilds IAM into. A replay
therefore reproduces a recorded refusal only when it is configured as the recording
was; a configuration difference surfaces as a divergence rather than being hidden.

### A seeded outcome replays under the same seed

A seed is recorded as an event of its own and re-applied where it was written, so a
stream recorded under a seed replays under the same seed (#1140). Three things follow,
and each was wrong before:

- **Position is preserved.** A seed written between two requests is re-applied between
  those same two requests, so a write the recording accepted before the seed was armed
  is accepted on replay too.
- **A consumed count restarts.** A budget the recording spent down to zero — an
  observation countdown, a conflict allowance — is re-armed at its seeded value, because
  the replay's reset wipes the counter and the recorded write re-arms it. The replay
  reproduces the recording's sequence rather than continuing from where it stopped.
- **A `DELETE` replays as the same `DELETE`.** The recorded event carries the query
  string, so clearing one seed does not replay as clearing every seed.

Before this, the seed lived only in the state manager, and a replay opens by resetting
the state manager. So a recording in which a conditional PUT was refused twice and then
accepted replayed as three acceptances, with nothing reported: every event was
re-executed and every one succeeded.

**A replay driven programmatically must be given the handler.** Re-applying a seed means
re-issuing the HTTP request that wrote it, and the engine needs something to issue it
against:

```go
engine := substrate.NewReplayEngine(store, state, tc, registry, cfg, logger,
    substrate.WithControlPlaneHandler(ts.ControlPlaneHandler()))
```

An engine given none counts each control-plane event in `SkippedEvents` and replays the
unseeded behaviour — the pre-#1140 outcome, now visible in the counters instead of
silent. Withholding it is occasionally what a test wants: a suite proving that a seed
only ever changed what an observation *reported*, and never the record underneath, can
replay without the handler and assert the records report their own settled states. That
is a deliberate choice to state in the test, not a default to fall into. `substrate
replay` wires the handler itself.

Not every control-plane write is recorded — only the ones that seed state an AWS
observation later reads. A replay resets the state manager, freezes the clock and rewinds
the fault controller itself, so `/v1/state/reset`, `/v1/control/time`, `/v1/control/scale`
and `/v1/fault/rules` are excluded; the remaining endpoints write nothing a replayed
observation can read. See
[How a seed survives a replay](/reference/concepts/seeds-and-replay#how-a-seed-survives-a-replay) for the rule as
the service reference states it.

### What a replay compares

Once an event has been re-executed, four comparisons run and each records its own
`EventDifference`: the state hash before the event and after it (only with
`validate_state`, and only for an event that carries one), the error, the status code,
and the **response body** — path by path (#817). Until #817 the body was not compared
at all, so two runs that agreed on `200` and differed on every value inside the
document were reported as matching. Response *headers* are not compared.

A body difference names where it is: `response_body` followed by the path within the
document, alongside the sequence number and the operation.

```
  Differences: 1
    - seq 3 ListBuckets response_body/ListAllMyBucketsResult/Buckets/Bucket[2]/Name [major]
      recorded: orders-archive
      replayed: orders-archives
```

JSON paths follow **JSON Pointer** (RFC 6901) — slash-separated, array indices
zero-based, `~1` for a `/` in a member name. XML paths follow **XPath** — a repeated
sibling carries a one-based `[n]`, an attribute carries `@`. Each format gets the
syntax its own ecosystem already has, which is why the two differ in index base.

Three normalisations are applied, and **nothing else**:

| Normalised | Why |
|---|---|
| JSON member order | An object is a map; two orderings are one document to every SDK |
| XML whitespace between elements | Indentation, not content. Non-whitespace text is always compared |
| A body that parses as neither format | Compared as bytes, so a CBOR document or an object payload is reported whole rather than as a parse failure |

Everything else is reported as it stands. In particular:

- **Collection order is a difference.** An array element and an XML sibling are
  matched by position. Sorting either side first would hide exactly the defect #864
  fixed — a listing rendered in Go map order — which *is* a body that differs between
  two runs of one input.
- **A minted identifier is a difference.** A resource id, an ARN, an access key or a
  secret minted during the recording is re-minted differently on replay, and the
  comparison reports it by path. This is the practical consequence to know about:
  **a recording that creates a resource reports body differences today**, and that is
  an honest report of [#856](https://github.com/scttfrdmn/substrate/issues/856) rather
  than a defect in the comparison. Masking it would report a reproduced run that was
  not one, which is the failure mode the whole verification exists to prevent.
- **A timestamp is expected to match.** The engine sets the simulated clock to the
  recorded event's timestamp before re-executing, so a body rendered from that clock
  reproduces. One that does not is a finding about the clock.

A body difference is graded `major`, never `critical`. The critical band is for a
divergence in *outcome* — a different state, or a refusal that replayed as a success —
because those make a passing test meaningless; a difference in a reported value is
what this comparison exists to surface. At most twenty differences are reported per
body, and when the walk stops it appends a difference saying so rather than leaving a
truncated list that looks complete.

The comparison runs whether or not `validate_state` is on. The two answer different
questions, and a read is where they come apart: a listing that rendered the wrong
value writes nothing, so its state hash is untouched and no hash comparison — even an
enabled one — can see it.

### Replaying from the command line

`substrate replay <stream>` replays a recorded stream outside a Go test. It needs a
persistent event store — a `memory` backend holds nothing from a previous process —
and `include_bodies`, which is on by default, because an event recorded without its
request cannot be re-executed and is reported as skipped:

```yaml
event_store:
  enabled: true
  backend: "file"
  persist_path: "/var/lib/substrate"
  include_bodies: true      # the default
  max_body_bytes: 1048576   # the default
  # Needed only for replay.validate_state below. A hash is a full snapshot of
  # state, taken twice per request, so it is off by default.
  include_state_hashes: true

replay:
  # Scales the recorded delay between events. Default 0, which replays instantly
  # and is the only value independent of the wall clock; 1.0 replays at the
  # original pace.
  speed_multiplier: 0
  # Stop at the first failing event. Default false, so the summary reports every
  # failure in the stream.
  stop_on_error: false
  # Compare a state hash before and after each event against the hash recorded
  # with it. Default false.
  validate_state: true
  # Start from the nearest stored snapshot instead of replaying from empty state.
  # Default false; a snapshot skips the events it already covers.
  use_snapshots: false
```

Until #880 this section did not exist, and the command built its engine with a
zero-valued `ReplayConfig` — so `validate_state` could not be switched on by any
means, and a replay reported how many events it re-executed without ever checking
that it reached the same state.

The state verdict in the summary distinguishes three outcomes, because two of them
are not verdicts at all:

```
  State:    valid                                          # hashes recorded, hashes matched
  State:    MISMATCH (2 error(s))                           # each one described on the line below
  State:    not checked (replay.validate_state is off)      # nothing was compared
```

A fourth line — `not checked (no event in the stream carries a recorded state
hash…)` — is what you get with `validate_state: true` over a stream recorded
without `include_state_hashes`. `StateValid` starts `true` and is only ever
falsified by a comparison, so reporting that run as "valid" would be a pass nothing
produced.

The `state:` section selects the state manager for both the server and the replay
engine, through one constructor, so the two cannot disagree about which backend is
in use. `memory` is the only implemented backend; anything else — `sqlite`
included, which is [#2](https://github.com/scttfrdmn/substrate/issues/2) — is
refused when the config loads, naming the backend. Before #881 an unimplemented
value was accepted and answered with a memory manager, so a config asking for
persistence got none and no error. `state.path` is refused for the same reason:
no implemented backend reads it.

<!-- TODO(#178): add section on persisting streams to SQLite for cross-run replay -->

## Time-Travel Debugging

Time travel rebuilds the state a stream had at any point by replaying it into a
`ReplaySandbox`: a fresh emulator built from the recording server's configuration,
with its own state, clock and plugins, so the live emulator is never reset. Each event
replays with its recorded request ID, clock reading and principal, so the state names
the identifiers the live run minted, and recorded seeds are re-applied.

A **position** is an index into one stream. The state at position `p` is the state
before the stream's event `p` ran: position 0 is the start of the run and
`len(stream)` is its end. A **sequence** (`Event.Sequence`) is numbered across the
whole store, so the events of one stream do not have consecutive sequences. The
engine takes positions and the HTTP and CLI surfaces take sequences.

```go
ctx := context.Background()
// Built as ts was: its configuration and its quota, consistency and fault controllers.
// Outside a test, substrate.NewReplaySandbox(ctx, cfg, store, logger) builds one from
// the configuration the recording server ran with.
sandbox := ts.ReplaySandbox(t)
engine := sandbox.Engine()

// Replay to the end, recording differences and snapshots.
results, err := engine.Replay(ctx, substrate.DefaultStreamID)
if err != nil {
    t.Fatal(err)
}
substrate.AssertReplayMatches(t, results)

// Move to just before the stream's fourth event.
if err := engine.JumpToEvent(ctx, 3); err != nil {
    t.Fatal(err)
}

// Step back over the third event. The returned event is the one now next to run.
prev, err := engine.StepBackward(ctx)
if err != nil {
    t.Fatal(err)
}
t.Logf("before %s/%s", prev.Service, prev.Operation)

// Read one namespace at this position.
s3State, err := engine.InspectState(ctx, "s3")
if err != nil {
    t.Fatal(err)
}
for key, val := range s3State {
    t.Logf("  s3/%s = %s", key, val)
}
```

The navigation methods:

| Method | Does |
|---|---|
| `JumpToEvent(ctx, position int) error` | Moves to `position`, from 0 to `len(stream)`. |
| `JumpToSequence(ctx, sequence int64) error` | Moves to just before the first event whose sequence is `sequence` or greater. |
| `StepBackward(ctx) (*Event, error)` | Moves back one event and returns it. |
| `StepForward(ctx) (*Event, error)` | Runs the next event and returns it with its replay error, if any. |
| `InspectState(ctx, namespace) (map[string][]byte, error)` | Reads a namespace at the current position; a failed read is an error. |

State cannot run backwards, so a backward move restores the nearest snapshot at or
before the target and replays forward from there, or resets and replays from the start
when there is none. Once a replay has moved backward, the engine takes a snapshot every
`event_store.snapshot_interval` events it replays (default 100), of the state manager and
of what plugins keep outside it, such as S3 object bytes. A replay that only moves
forward takes none. Snapshots only shorten a jump: none is taken while fault rules are
armed, or when a plugin's state cannot be captured, and a jump reaches the same state
without one. An event re-run on the way to a target does not report its differences a
second time, and an event's replay error does not stop a jump.

`ReplaySandbox.StateAt(ctx, stream, sequence)` combines these: it returns the state
before `sequence` as a `StateAt`, with the differences, failures and skips of the
events that ran before it. `Client.NewDebugSession(stream)` wraps a sandbox as well,
so its `JumpToEvent` and `StepBackward` never touch the client's own state.

### Over HTTP and from the CLI

The same path answers `GET /v1/debug/events/{seq}/state`, and the debug UI's state
panel:

```bash
# The state before the event with sequence 12, in the default stream.
curl -s 'http://localhost:4566/v1/debug/events/12/state'
# {"stream":"default","sequence":12,"events_applied":7,"state":{…},
#  "differences":[],"failed_events":0}

# Another stream, and the change between two points.
curl -s 'http://localhost:4566/v1/debug/events/40/state?stream=my-stack'
curl -s 'http://localhost:4566/v1/debug/state/diff?from=0&to=40'
```

`differences` lists every place the replay diverged from the recording. An empty list
means the state is the one the live emulator had. A stream with no events is a 404. The
diff endpoint reports keys as `namespace/key`, with values base64-encoded.

`substrate debug state-at <seq> [--stream S]` prints the same JSON from a persistent
event store, without a running server:

```bash
substrate debug state-at 12 --config substrate.yaml
substrate debug state-at 40 --stream my-stack --config substrate.yaml
```

## Regression Fixtures

`GET /v1/debug/export?stream=S` (or `substrate.GenerateTestFixture(events, pkg, test)`)
writes a Go test file that replays a recorded stream and fails on any difference. The
file embeds the recorded events as JSON and replays them in process with
`ts.ReplayEvents(t, events)` against a fresh `StartTestServer`, then asserts with
`substrate.AssertReplayMatches`. Because nothing is re-sent over HTTP, a fixture keeps
what an HTTP re-send loses: the recorded request IDs and every identifier minted from
them, the recorded clock, the method, the query parameters, `X-Amz-Target` and the
other recorded headers, and the parsed form of a Query-protocol request.

```bash
curl -s 'http://localhost:4566/v1/debug/export?stream=default&package=mypkg_test&test=TestCheckout' \
  > checkout_replay_test.go
go test -run TestCheckout ./...
```

The fixture's test server is started with `WithoutDefaultVPC()` when the recording
server ran without a default VPC, with `WithLenientCloudFormation()` when it ran with
CloudFormation's strict mode off, and with no other option. It arms no fault rule and
enforces no quota or consistency, so a stream recorded under armed fault rules is refused
with a 422; a recorded throttle or `InconsistentStateException` replays as a difference.
The generated file declares an empty allowlist (`<test>Allow`) of `path.Match`
patterns for fields a fixture may let differ. A stream holding an event that cannot be
replayed — recorded without bodies, or with a request body over the cap — is refused
with a 422 rather than exported as a fixture that cannot pass. After a replay the test
server holds the replayed state, so a fixture can carry on with assertions of its own.

<!-- TODO(#178): document SetBreakpoint API once exposed -->

## Counting Requests From Another Process

A Go test that starts the emulator itself can count requests through the event
store: `ts.Store().GetEvents(ctx, substrate.EventFilter{Service: "s3", Operation:
"PutObject"})` returns every matching event.

A consumer that runs `substrate server` as a **separate** process cannot reach that
store. Read the same log over HTTP instead:

```bash
# How many PutObjects has this server seen?
curl -s 'http://localhost:4566/v1/debug/events?service=s3&operation=PutObject'
# {"events":[…],"count":2,"total":2,"truncated":false}
```

`operation=` (or `op=`) filters to one operation, and `service=`, `stream=` and
`after=` narrow further. Each entry carries at least `seq`, `service`,
`operation`, `status_code`, `error_code` and `timestamp`.

**Assert on `total`, not on `count`.** `count` is the length of `events`, which
`limit=` (default 500) trims to the *most recent* matches — so on a run that
recorded more than the limit, a count read from `count` is silently short and a
request early in the run is not in the page at all. `total` is the number matching
the filter before the limit, and `truncated` says whether the two differ.

```bash
# Nothing wrote, and that is a claim about the whole run rather than the last page.
test "$(curl -s 'http://localhost:4566/v1/debug/events?op=PutObject' | jq .total)" -eq 0
```

This is stronger than diffing a bucket listing before and after, which cannot see a
PUT that rewrote an object with identical bytes, and cannot tell "nothing was
attempted" from "something was attempted and refused". `status_code` tells those
apart — it is recorded whether or not `event_store.include_bodies` is set, and so is
`error_code`, which carries the refusal's own AWS code.

Two members are **not** in an entry: `method` and `path`. They live on the recorded
request, which `event_store.include_bodies` governs, so they would be empty with it
turned off; `service` plus `operation` identifies an operation in every
configuration.

From the CLI, `substrate inspect <service>` prints the most recent 100 events for one
service and says `showing 100 of N` when the run was longer.

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

The code is `AccessDenied`, and so is an identity-policy denial *on the same
service* — the two gates agree. The `sqs list-queues` denial above reports
`AccessDeniedException` because the code follows the service's **wire protocol**,
not which gate refused: AWS sends the bare form on the XML protocols (Query,
REST-XML, EC2 — so STS, IAM, CloudFormation, S3) and the suffixed form on the JSON
ones (so SQS, DynamoDB, Lambda). An explicit `Deny` in the trust policy is
reported with a different message ("with an explicit deny in the role trust
policy") under the same code.

**Writing a trust policy is the opt-in**, the same shape as the rule below one
level down. A role created without one is not enforced, so every test that never
wrote a trust policy keeps working — see
[the STS service reference](/reference/sts#a-role-s-trust-policy-is-enforced-and-sts-externalid-with-it)
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
as a `StackEvent`. See
[the CloudFormation reference](/reference/cloudformation) for the `RoleARN` semantics.

## CloudFormation Strict Mode

A test server checks every template the way CloudFormation does before anything
deploys, and by default goes one step further: a resource type substrate does not
emulate is **refused**, rather than deployed as a stub that reports `CREATE_COMPLETE`
for a resource nothing created. A green test therefore means the template deployed
real resources a later API call can find.

```go
ts := emulator.StartTestServer(t)
// AWS::EKS::Cluster has no deploy helper, so CreateStack answers ValidationError:
// "Template declares resource types substrate does not emulate: [AWS::EKS::Cluster (Cluster)]. …"
```

What is refused in every mode, because CloudFormation refuses it: a type the
CloudFormation resource specification does not list (`AWS::S3::Bukcet` is
`Template format error: Unrecognized resource types: [AWS::S3::Bukcet]`), a `Ref`,
`Fn::GetAtt` or `Fn::Sub` naming something the template does not declare, and a
parameter with no `Type` or an unrecognized one. A property the resource's type does not
declare (`VisibilityTimeoutt`) fails that resource with `CREATE_FAILED`, and the stack
rolls back. `ValidateTemplate` makes the same checks without deploying anything.

A template that genuinely needs a type substrate does not emulate — or a custom resource,
which substrate cannot run — can opt out:

```go
ts := emulator.StartTestServer(t, emulator.WithLenientCloudFormation())
```

The server-side equivalent is `cloudformation.lenient: true` (or `cloudformation.strict:
false`) in `substrate.yaml`, or `SUBSTRATE_CLOUDFORMATION_LENIENT=true`
(`SUBSTRATE_CLOUDFORMATION_STRICT=false`). In process, pass `emulator.WithDeployerLenient(true)`
to `NewStackDeployer` or `emulator.WithClientLenientCloudFormation()` to `NewClient`. In
lenient mode an unemulated type records its properties and creates nothing, as every
template's did before strict mode existed; the checks CloudFormation itself makes still
apply. See [Strict mode and template validation](/reference/cloudformation#strict-mode-and-template-validation)
for every check and where its wording comes from.

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

## Testing from Python (pytest-substrate)

The [`pytest-substrate`](https://github.com/scttfrdmn/substrate/tree/main/python) plugin runs the
substrate binary for pytest. [First Python test](/getting-started#first-python-test-pytest) covers
installing it; this section covers the three things a test of infrastructure code needs beyond a
client that reaches the emulator.

| Fixture | Server | State |
|---------|--------|-------|
| `substrate` | one per session | reset before each test |
| `substrate_isolated` | a fresh process per test | its own |

Both set `AWS_ENDPOINT_URL`, `AWS_DEFAULT_REGION` and test credentials, so `boto3.client("s3")`
reaches the emulator with no arguments, and both yield a `SubstrateServer` whose `url` is the
server's address.

**Deterministic runs.** Every server the plugin launches gets `SUBSTRATE_RUN_SEED=1`, so request
IDs and the identifiers minted from them repeat from run to run, and the per-test reset rewinds the
sequence. To pin the clock too, mark the test; `substrate_isolated` starts its server pinned, and
`substrate` pins the shared server for that test alone:

```python
import pytest

@pytest.mark.substrate(deterministic=True)       # clock frozen at 2026-01-01T00:00:00Z
def test_report_is_stable(substrate_isolated):
    ...

@pytest.mark.substrate(epoch="2026-06-01T12:00:00Z", seed=7)
def test_at_noon(substrate):
    ...
```

Indirect parametrization takes the same keys (`{"deterministic": True}`), and the session server
reads the ini options `substrate_seed`, `substrate_epoch` and `substrate_deterministic`.
`SubstrateServer(seed=..., epoch=...)`, `set_run`, `run_settings`, `freeze_time` and
`unfreeze_time` are the same controls by hand; see [Deterministic Runs](#deterministic-runs) for
what each setting does and what stays nondeterministic.

**Seed an outcome** through the control plane before the call that observes it. Substrate runs no
workload, so a failed command, a terminal job state or a capacity error is something a test sets
rather than waits for:

```python
import json, urllib.request

def test_failed_health_check(substrate_isolated):
    body = {"documentName": "AWS-RunShellScript", "status": "Failed", "stderr": "connection refused", "exitCode": 7}
    req = urllib.request.Request(f"{substrate_isolated.url}/v1/ssm/command-invocation",
                                 data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req)
    # ... SendCommand now reports Failed, and the code's failure path runs.
```

`seed_result`, `set_status` and `clear_seeds` wrap the query-result endpoints (Athena, Redshift
Data); other seeds are posted as above.

**Move the simulated clock** instead of sleeping. `set_time` sets it (`POST /v1/control/time`),
`advance_time` moves it forward from where it is, and `now` reads it; all three answer an aware UTC
`datetime`. Work that falls due as time passes — a Scheduler schedule, a lease — runs on the next
request after the clock passes it:

```python
import datetime

def test_window_opens(substrate_isolated):
    substrate_isolated.set_time(datetime.datetime(2026, 11, 16, 9, tzinfo=datetime.UTC))
    # ... create a one-time schedule for 10:00 ...
    substrate_isolated.advance_time(hours=1, seconds=5)
    # ... the schedule's target has run.
```

The clock keeps running from the instant set, so assert a window (`start <= t < start + 1 minute`)
rather than an exact timestamp.

**Assert on state** through the same APIs the code under test uses, rather than through
substrate's internals, so the test means the same thing against AWS.

[`python/examples/test_journey.py`](https://github.com/scttfrdmn/substrate/blob/main/python/examples/test_journey.py)
puts the three together around a small release script, and runs in CI with the plugin's own tests.

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
    // that cross-service dispatch is approximated; see docs/reference/sqs.md).
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
