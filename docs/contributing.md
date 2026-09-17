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
which would break deterministic replay.

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
goroutine, a container, a buffer of bytes the `StateManager` only holds metadata for.
The test is whether a reset leaves the thing unreachable: if the only call that stops
it looks up a record the reset just deleted, then after the reset nothing short of
`Shutdown` can reach it, and it keeps acting on the next run (#902, #903). Three
in-tree cases, each with the consequence that made it a defect:

- **S3's object payloads.** The bytes live in an `afero` filesystem on the plugin and
  every read of them is gated on metadata the reset deletes, so they were unreachable
  and unreleased for the life of the process. `ResetForRun` empties the filesystem —
  by removing each root entry, because `RemoveAll("/")` on a `MemMapFs` reports
  success, leaves every file beneath readable and destroys the root entry.
- **Lambda's event-source-mapping pollers.** `DeleteEventSourceMapping` is the only
  call that closes a poller's stop channel, and it answers `ResourceNotFoundException`
  once the record is gone. The goroutine polls on a wall-clock ticker, and both the
  queue URL and the function ARN are name-derived, so the poller consumed the *next*
  run's messages and invoked its function.
- **RDS's Postgres containers.** The container handle is read back out of the
  `StateManager`, so a reset made the container unstoppable through the API while it
  still held the `substrate-rds-<id>` Docker name the next `CreateDBInstance` needs.

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
`ssm_control.go`, `spawn_control.go`, Athena/SageMaker/Bedrock seeds for
worked examples, and register the routes in `server.go`'s `buildRouter`.

### Registration + service reference

Register the plugin in `RegisterDefaultPlugins` (`emulator/plugins.go`). Then add
a metadata entry (display name + protocol) in
[`cmd/gen-service-reference`](https://github.com/scttfrdmn/substrate/tree/main/cmd/gen-service-reference)
and regenerate the reference:

```bash
make docs-reference
```

The `docs-reference-check` CI job **fails** if a registered plugin has no
metadata entry, so this is not optional.

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

## Checklist before opening a PR

- [ ] There is a tracked issue for the work.
- [ ] Behaviour passes the scope test (API-observable, not resource-internal).
- [ ] Shapes/errors/pagination verified against the official AWS API docs.
- [ ] Alternate outcomes are seedable via a control-plane endpoint, not random.
- [ ] Plugin registered in `RegisterDefaultPlugins` **and** metadata added to
      `cmd/gen-service-reference`; `make docs-reference` run.
- [ ] Tests added (table-driven, race-safe, no wall-clock/network), coverage
      maintained.
- [ ] `make test` and `make lint` pass.
- [ ] `CHANGELOG.md` updated under `## [Unreleased]`.
- [ ] Every exported symbol has a doc comment ending in a period.
