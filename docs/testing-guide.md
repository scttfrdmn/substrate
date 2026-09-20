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
        // Lets a recorded seed be re-applied during the replay; see "A seeded
        // outcome replays under the same seed" below.
        substrate.WithControlPlaneHandler(ts.ControlPlaneHandler()),
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

### What a replay re-executes

A live request passes through nine pipeline steps before its response is written, and a
replay does not re-run all of them. Four are re-decided, four are skipped, and one is
replaced — each for a reason about what the recording settled and what it did not
(#833):

| Step | On replay |
|---|---|
| Authorization (`AuthController.CheckAccess`) | **re-decided**, against the principal the event records |
| Quota (`QuotaController.CheckQuota`) | **consulted, and exempts a replay** |
| Consistency (`ConsistencyController.CheckRead`) | **consulted, and exempts a replay** |
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

Quota and consistency are consulted rather than skipped so that the exemption lives in
the controllers, where it was always written. Consistency has the stronger reason:
`RecordWrite` is exempt on replay too, so no propagation window ever opens and a
`CheckRead` that enforced would be refusing against a window that does not exist.

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
[How a seed survives a replay](services.md#how-a-seed-survives-a-replay) for the rule as
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
and `include_bodies`, because an event recorded without its request cannot be
re-executed and is reported as skipped:

```yaml
event_store:
  enabled: true
  backend: "file"
  persist_path: "/var/lib/substrate"
  include_bodies: true
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
  # Seeds the engine's random source. Default 0, which leaves it unseeded.
  random_seed: 0
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
request, which `event_store.include_bodies` governs, so they would be empty in the
default configuration; `service` plus `operation` identifies an operation in every
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
