# Contributing a Service Plugin

This guide walks through adding a new AWS service plugin to Substrate and the
conventions your change must follow. It assumes you have read
[Scope & Philosophy](/scope) — the scope test below is the first gate for any
new behaviour.

For general project rules (Go style, changelog, release process) see
[`CLAUDE.md`](https://github.com/scttfrdmn/substrate/blob/main/CLAUDE.md). All
non-trivial work starts with a
[GitHub issue](https://github.com/scttfrdmn/substrate/issues) tied to a
milestone.

## Before you write code: the scope test

Substrate models **what is observable through an AWS API call, not what software
inside a resource does.** Before adding a behaviour, ask:

> *Is this observable through an API call, or is it resource-internal?*

- **In scope:** request/response shapes, error codes, resource state and its
  transitions over the simulated clock, pagination, and **seedable** outcomes
  (errors, capacity failures, terminal job states, specific result sets).
- **Out of scope:** actually running the workload — executing user-data,
  running a Lambda's code, performing an inference or training job. Capture such
  inputs as recorded intent and expose a **seedable** completion signal instead.

If a feature needs to model box-internals, it belongs in a different tool or test
tier, not in Substrate.

## Anatomy of a plugin

A plugin implements the `Plugin` interface (`emulator/types.go`):

```go
type Plugin interface {
    Name() string                                                    // AWS service name for routing, e.g. "s3"
    Initialize(ctx context.Context, config PluginConfig) error       // wire up state, logger, time controller
    HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error)
    Shutdown(ctx context.Context) error
}
```

A complete, runnable minimal plugin lives in
[`examples/custom_plugin`](https://github.com/scttfrdmn/substrate/tree/main/examples/custom_plugin)
— start there. The essentials:

```go
type WeatherPlugin struct {
    state  emulator.StateManager
    logger emulator.Logger
}

func (p *WeatherPlugin) Name() string { return "weather" }

func (p *WeatherPlugin) Initialize(_ context.Context, cfg emulator.PluginConfig) error {
    p.state = cfg.State
    p.logger = cfg.Logger
    return nil
}

func (p *WeatherPlugin) HandleRequest(_ *emulator.RequestContext, req *emulator.AWSRequest) (*emulator.AWSResponse, error) {
    switch req.Operation {
    case "GetWeather":
        return p.handleGetWeather(req)
    default:
        return nil, &emulator.AWSError{
            Code:       "UnsupportedOperation",
            Message:    fmt.Sprintf("operation %q is not supported", req.Operation),
            HTTPStatus: http.StatusBadRequest,
        }
    }
}

func (p *WeatherPlugin) Shutdown(_ context.Context) error { return nil }
```

If your plugin is time-dependent, pull the `*TimeController` from
`cfg.Options["time_controller"]` in `Initialize` (see any built-in plugin such
as `ssm_plugin.go`) and read the clock with `p.tc.Now()` — never `time.Now()`,
which would break deterministic replay. Mint identifiers through the request's
`IDMint` (`reqCtx.IDs.Hex(n)`, `.UUID()`, `.Chars(n, alphabet)`), never from
`math/rand` or `crypto/rand`. `make nondeterminism-check` refuses either outside
the allowlist in `scripts/check-nondeterminism.sh` (#1600).

## Conventions

### Verify against the real AWS API

Request/response shapes, error codes, pagination, and IAM condition keys must be
checked against the **official AWS API reference** for the service — not built-in
knowledge. This is a hard rule (`CLAUDE.md` → AWS service emulation).

### State keys

Persist state through `StateManager` under a namespace (usually the service
name) with a stable, documented key shape, e.g. `object:{bucket}/{key}` for S3
or `queue:{account}/{name}` for SQS. Keep keys consistent across the plugin's
handlers so `ResetState` and replay behave predictably.

### Mutable state a plugin keeps on itself

Anything a plugin mutates should live in the `StateManager`, because that is what a
reset clears. If your plugin must keep mutable state on its own struct — a sequence
counter an identifier is minted from, a cache, a random source — implement the
optional `ResettablePlugin` interface as well:

```go
func (p *WeatherPlugin) ResetForRun(_ context.Context) error {
	p.forecastSeq.Store(0)
	return nil
}
```

`PluginRegistry.ResetPlugins` calls it for every plugin that implements it, and both
reset paths — `ReplayEngine.resetState` at the start of a replay and
`POST /v1/state/reset` — go through it. It is optional precisely so the plugins that
keep everything in the `StateManager` need no method at all.

Without it, a counter carries across a reset and the identifiers your plugin mints
depend on how many runs preceded them in the process: replaying one recorded stream
twice produced different values from identical events, which is the property the
event log exists to rule out (#886). `ResetForRun` must be safe to call on a plugin
that has never handled a request, and safe to call while requests are in flight.

The hook also owes whatever the plugin **started**, not only what it minted — a
goroutine, a poll schedule, a buffer of bytes the `StateManager` only holds metadata for.
The test is whether a reset leaves the thing unreachable: if the only call that stops
it looks up a record the reset just deleted, then after the reset nothing short of
`Shutdown` can reach it, and it keeps acting on the next run (#902, #903). Two
in-tree cases, each with the consequence that made it a defect:

- **S3's object payloads.** The bytes live in an `afero` filesystem on the plugin and
  every read of them is gated on metadata the reset deletes, so they were unreachable
  and unreleased for the life of the process. `ResetForRun` empties the filesystem —
  by removing each root entry, because `RemoveAll("/")` on a `MemMapFs` reports
  success, leaves every file beneath readable and destroys the root entry.
- **Lambda's event-source-mapping pollers.** `DeleteEventSourceMapping` is the only
  call that closes a poller's stop channel, and it answers `ResourceNotFoundException`
  once the record is gone. Both the queue URL and the function ARN are name-derived,
  so the poller consumed the *next* run's messages and invoked its function.

One exception, and it is about ownership rather than reachability: state the **caller**
handed the plugin is the caller's to clear. S3 empties its filesystem only when it
created it, because an injected `Options["filesystem"]` may be backed by a directory
holding fixtures the caller means to keep, and only the caller knows that. Nothing can
inject one over HTTP, so this is reachable only by an in-process embedder.

Where the state is invisible through the wire — payload bytes, a live goroutine — the
assertion needs a test-only accessor in `export_test.go` rather than a request, and its
doc comment should say why no request can see it. Prefer a deterministic form: assert
the poller *count*, not that polling stopped, because a poll happens on a wall-clock
tick and no test may depend on real elapsed time.

### Errors

Return `*AWSError` with the exact AWS `Code` and HTTP status for API-level
errors. Wrap internal Go errors with context (`fmt.Errorf("...: %w", err)`) and
never discard them.

### Seedable outcomes (the deterministic-emulator pattern)

A new operation defaults to its nominal success path. Alternate outcomes
(errors, capacity failures, terminal states, specific result sets) are exposed
as **seedable** values a test sets via a control-plane endpoint, read at request
time — never as nondeterministic behaviour. Follow the established pattern:
`POST`/`DELETE /v1/{service}/...` keyed by an ID or the `"*"` wildcard. See
`ssm_control.go`, `lambda_control.go` (`/v1/lambda/invoke-result`),
`s3_seeded_objects.go` (`/v1/s3/seeded-objects`), Athena/SageMaker/Bedrock seeds for
worked examples, and register the routes in `server.go`'s `buildRouter`.

### Registration + service reference

Register the plugin in `RegisterDefaultPlugins` (`emulator/plugins.go`) by adding a
row to its table. The middle string of a row is only an error label; the registry key
is whatever `Name()` returns. `pluginWiring.register` always injects
`"time_controller"`. A plugin that dispatches to other plugins also takes
`registryOpt` (`{"registry": registry}`), and one that runs IAM checks takes the auth
controller via `WithPluginAuth`.

A new plugin is not done until every table below agrees with the registry. Each row
is enforced by a test or a CI gate, so a missing one fails the build rather than
drifting silently.

| What | Where | Enforced by |
|------|-------|-------------|
| Routing identifiers (Display, Protocol, TargetPrefix, Hosts, SigningNames, Source) | `emulator/routing.go` `pluginRouting` | `routing_test.go` (both directions, and every host/signing name must parse to a registered plugin); `cmd/gen-service-reference` refuses a plugin with no entry |
| Error protocol | `emulator/error_protocol.go` (e.g. `"elasticloadbalancing": errProtoQueryXML`) | `access_denied_code_test.go`; `unknownActionError` uses it to answer `InvalidAction`/`UnknownOperationException` |
| Operation catalog | `make operation-catalog` regenerates `emulator/operation_catalog_gen.go` from each `HandleRequest` switch (on a variable named `op`, `action` or `operation`) | `make operation-catalog-check` |
| Catalog totals | `routedOperationTotal`, `routedPluginTotal` and the per-service counts in `emulator/operation_catalog_test.go` — bumped by hand | `operation_catalog_test.go` |
| Service section | a `## <Section>` in `docs/services.md` with one *Supported operations* row per routed operation, plus a `docSections` entry in `cmd/check-operation-docs/main.go` | `make operation-docs-check` (`scripts/undocumented-operations.txt` is empty and stays empty), `make docs-headings-check` |
| Coverage matrix | `make docs-reference` regenerates the matrix at the top of `docs/services.md` | `make docs-reference-check` |
| Wire bookkeeping | a state struct with a json-tagged `AccountID`, `Region`, `CreatedAt`, `UpdatedAt` or `EverTagged` needs lines in `scripts/wire-bookkeeping-baseline.txt` and in `-projected.txt` (naming a raw-bytes test proving the field never reaches the wire — `ecr_wire.go` is the pattern) or `-internal.txt` | `make wire-bookkeeping-check` |
| `ResettablePlugin` list (only if you implement it) | `emulator/replay_plugin_reset_test.go` | the test asserts the exact set |
| Costs (optional) | `defaultCostTable` in `emulator/costs.go` | unknown operations cost 0 |
| Authz reference (optional) | `cmd/gen-authz-reference` + `emulator/authzref/*.json` | `make authz-reference-check` (offline) |
| CloudFormation resource type (only for a new CFN type) | a row in `cfnResourceDeployers` (`emulator/cfn_resource_table.go`), the one list strict mode reads | `TestCFNStrict_EmulatedTypesAreRecognized` (the type must be in the vendored specification, `emulator/cfnschema`, checked by `make cfn-schema-check`) |

Adding an operation to an existing plugin needs the catalog, totals, service-section
row and coverage-matrix steps.

CFN resources live in `emulator/cfn_resources_<service>.go`: each service's deploy, update and
delete helpers together. Files are named by domain, never by release (`TestFileNames_AreNamedByDomainNotRelease`).

### Models to copy

Each plugin owns its wire encoding, so start from the closest existing one rather than
from scratch.

- **awsQuery (XML) services** — copy ELB. `elb_plugin.go` dispatches on
  `req.Operation` with `unknownActionError` as the default; `query_envelope.go`
  (`queryOKResponse`) writes the `<{Op}Response>` envelope with `ResponseMetadata` carrying
  `reqCtx.RequestID` for every awsQuery plugin. Pass `nil` as the result for an operation whose
  page shows no result element, and an empty struct for one whose page shows it empty. Read `member.N` lists with
  `extractIndexedParams` (`ec2_plugin.go`) and structs with `iamMemberList`,
  `iamMemberStructs` and `iamMemberTags` (`iam_query.go`). Errors are plain `*AWSError`
  values; the shared Query writer (`marshalAWSError` in `error_protocol.go`) emits the
  `<ErrorResponse>` with `<Type>` from `queryFaultType`.
- **JSON services** — copy ECR. `HandleRequest` switches on the `X-Amz-Target`
  suffix; responses go through a separate wire struct (`ecr_wire.go`) so bookkeeping
  fields never reach the wire.
- **Pagination** — `query_marker_pagination.go` (`Marker`/`MaxRecords`, RDS and
  ElastiCache), `ec2_pagination.go` (`NextToken`/`MaxResults`, `ec2Page[T]`),
  `elb_page_size.go`, and CloudFormation's offset tokens in `cfn_events.go`. An
  unreadable token is an error, never page one.

### Calling another plugin

No plugin holds a pointer to another. To act through another service, build an
`*AWSRequest` and send it through `registry.RouteRequest`, so the call is routed,
authorized and recorded exactly like a client's. CloudFormation's
`StackDeployer.dispatch` (`cfn_deployer.go`) is the reference: it mints a fresh
`RequestContext` with `NewIDMint(requestID)`, runs `auth.CheckAccess` when a principal
is present, routes the request and records the event. When the effect is launching
instances, EC2 Fleet (`ec2_fleet.go`: `authorizeFleetLaunches` then `launchFleetPool`)
is the precedent: authorize every launch before performing any. A reserved `aws:` tag
can be stamped by internal callers only (fleet stamps `aws:ec2:fleet-id`).

### Status progressions and clock-driven work

When a resource's published lifecycle passes through states one request cannot show,
use the shared seeded progression in `emulator/progression.go` (`newProgression`,
`observe`, `peek`, `reset`, and `serveSeed`/`serveClear` for the control-plane
handlers). It counts *observations*, not elapsed time, so "still pending" assertions
are exactly reproducible live and on replay. EC2 instances
(`ec2_instance_state_control.go`, `POST/DELETE /v1/ec2/instance-state`) are the worked
example: the stored record always holds the settled state, and the seed only governs
what an observation reports. Register seed endpoints inside the control-plane group
in `server.go` so `recordControlPlaneWrites` records them and they replay.

Work that must happen as simulated time passes, rather than on a request, implements
`ClockDrivenPlugin.RunDue` (`emulator/clock_driven.go`); the registry runs it before
every top-level request, live and on replay. Lambda's event-source mappings
(`lambda_esm_clock.go`) and EventBridge Scheduler's due schedules (`scheduler_clock.go`,
which also authorizes each dispatch as the target's role) are the users today — prefer
reconciling inside the request that causes the change, or on the next observation,
when the published behaviour allows it.

### Concurrency

Handlers run concurrently and `StateManager` has no compare-and-swap. An operation
that reads, checks and writes one resource (a conditional write, an index list
update) must hold a lock across all three. S3's striped `s3KeyMutex`
(`s3_conditional.go`, 64 FNV-hashed stripes) is the pattern; lock multiple keys in
sorted order.

## Testing

- Tests live in `package emulator_test` (`emulator/{service}_plugin_test.go`),
  are table-driven where practical, and must be race-safe.
- No test may depend on network access, real AWS, or wall-clock time — drive time
  through the `TimeController` / `TestServer.AdvanceTime`.
- Use `StartTestServer(t)` for integration-style tests over HTTP; it registers
  all plugins and cleans up automatically. See the [Testing Guide](/testing-guide).
- Aim for **>80% coverage**. `make test` runs the race detector; `make coverage`
  generates a report.
- If your change diverges from real AWS behaviour in any way, document it (and
  the reason) so reviewers can see it.

## Reproducing CI locally

Two CI jobs exist only to catch bookkeeping drift. Run their targets before pushing:

| CI job | Targets | Regenerate after a deliberate change |
|--------|---------|--------------------------------------|
| Service Reference Drift | `make operation-catalog-check docs-reference-check docs-headings-check operation-docs-check authz-reference-check cfn-schema-check` | `make operation-catalog docs-reference operation-docs-write` (and `make cfn-schema` after a `make cfn-schema-fetch`) |
| Wire Bookkeeping Drift | `make wire-bookkeeping-check discarded-unmarshal-check discarded-marshal-check discarded-state-check nondeterminism-check` | `make wire-bookkeeping-write` |

The `discarded-*` checks are lexical: no `_ = json.Unmarshal(req.Body…)`, no discarded
`json`/`xml.Marshal`, no discarded `state.Put`. So is `nondeterminism-check`: no wall-clock read
(`time.Now`, `time.Since`, `time.After` and their relatives), no `math/rand` or `crypto/rand`,
and no `generateRequestID` or `NewIDMint("")` in emulator code outside its allowlist. `make docs-versions` refuses a pinned
release number in README or `docs/` prose. `make e2e` runs the aws-sdk-go-v2 suite in
`test/e2e` (a separate module with `replace … => ../../`, so a root `go.mod` bump
needs `cd test/e2e && go mod tidy`), and `make python-test` runs pytest-substrate.

## Checklist before opening a PR

- [ ] There is a tracked issue for the work.
- [ ] Behaviour passes the scope test (API-observable, not resource-internal).
- [ ] Shapes/errors/pagination verified against the official AWS API docs.
- [ ] Alternate outcomes are seedable via a control-plane endpoint, not random.
- [ ] Every table in [Registration + service reference](#registration-service-reference)
      updated; the generators re-run.
- [ ] Tests added (table-driven, race-safe, no wall-clock/network), coverage
      maintained.
- [ ] `make test` and `make lint` pass, and both drift jobs' targets pass.
- [ ] `CHANGELOG.md` updated under `## [Unreleased]`.
- [ ] Every exported symbol has a doc comment ending in a period.
