// Package emulator provides an event-sourced AWS emulation layer for
// deterministic testing of AI-generated infrastructure code.
//
// # Overview
//
// Substrate's core differentiators:
//
//  1. Deterministic reproducibility: event sourcing + seeded RNG + time control
//     means the same test inputs always produce the same outputs.
//
//  2. Time-travel debugging: step backward through recorded request history
//     and inspect AWS service state at any point in time.
//
//  3. Cost visibility: real AWS pricing is tracked per operation so you know
//     projected monthly costs before the bill arrives.
//
//  4. API-surface scope: substrate models what is observable through an AWS API
//     call — request/response shapes, resource state and its transitions over
//     the simulated clock, and seedable outcomes — not what software inside a
//     resource does. It never executes the workload behind the API (user-data,
//     Lambda code, an inference, a training job); such inputs are captured as
//     recorded intent with a seedable result. This is also why replay is
//     deterministic: API observations are recordable and replayable, whereas
//     resource internals are not.
//
// # What determinism and replay give you
//
// Determinism and the event log are not ends in themselves; they unlock
// capabilities that flaky, stateful test infrastructure cannot offer:
//
//   - No flakes: the same inputs always produce the same outputs, so a green
//     test stays green and a red test is a real signal, not timing noise. CI
//     does not retry-until-pass.
//   - Exact reproduction: a failure replays identically from its recorded
//     events, so "works on my machine" and heisenbugs disappear — you debug the
//     exact run, not an approximation of it.
//   - Time-travel inspection: step backward through request history and read
//     resource state at any point, to see precisely where a sequence of API
//     calls diverged from what was expected.
//   - Testable rare paths: seeded outcomes make capacity failures, throttling,
//     terminal job states, and slow transitions instant and repeatable, so the
//     retry/poll/wait logic that only runs on the unhappy path is actually
//     covered (see Seeding, below).
//   - Fast and free: no network, no real account, no provisioning latency or
//     spend — suitable for unit tests and tight inner loops, and for validating
//     AI-generated infrastructure code before it ever touches AWS.
//   - Regression fixtures: a recorded run can be replayed as a fixture and
//     exported as a standalone test, turning a once-seen scenario into a
//     permanent guard.
//
// # Seeding: determinism without sacrificing coverage
//
// Determinism does not mean every test sees the same result. Seeding is the
// mechanism that lets a deterministic emulator produce different outcomes on
// demand. By default an operation returns its nominal success path; a test seeds
// an alternate outcome through a control-plane endpoint, and the plugin reads
// that seed at request time. The same launch can therefore be made to return
// InsufficientInstanceCapacity, a training job to come back Failed with a
// CapacityError, or a query to return a specific result set — each chosen by the
// test, each fully reproducible. Crucially, the failure, capacity, and timing
// paths a consumer's retry/poll/wait loops exist to handle are exactly the paths
// that are rare, slow, or impossible to trigger on demand against real AWS;
// seeding makes them first-class, instant, and deterministic.
//
// # Why determinism (vs. containers or real infrastructure)
//
// The same property is reachable three ways, with very different trade-offs:
//
//   - Real AWS / LocalStack-with-containers run actual workloads, so behavior
//     depends on wall-clock timing, process scheduling, network, and the live
//     state of a remote account. Failure and edge-case paths are hard to trigger
//     and rarely reproduce; a flake cannot be replayed.
//   - Hand-written mocks are deterministic but bespoke per test, drift from the
//     real API, and cannot model state transitions or be inspected over time.
//
// Substrate records every request as an immutable event over a simulated clock,
// so a run is reproducible by construction: the same inputs (including seeds)
// always yield the same outputs, a failing run replays exactly for debugging,
// and you can step backward through history to inspect state at any point — none
// of which a container-backed or real-infrastructure approach offers. The cost
// is fidelity to workload internals, which is deliberately out of scope (see
// differentiator 4): substrate is the fast, deterministic tier for exercising
// how code drives and reacts to the AWS API, not for validating what runs inside
// a resource.
//
// # Quick Start
//
// Use [BettyClient] to deploy a CloudFormation template, record operations,
// and validate the results in a single workflow:
//
//	betty := emulator.NewBettyClient(registry, store, state, tc, logger)
//
//	result, err := betty.Deploy(ctx, cfnTemplate, emulator.Intent{MaxCost: 1.0})
//	if err != nil { ... }
//
//	session, err := betty.StartRecording(ctx, "my-integration-test")
//	// ... run operations against the emulator ...
//	report, err := betty.StopRecording(ctx, session)
//
// See examples/betty_workflow/main.go for a complete runnable example.
//
// # Architecture
//
// All types live in a single package. The [EventStore] records every AWS
// request as an immutable [Event]. The [ReplayEngine] replays event streams
// for deterministic test validation and time-travel debugging.
//
// Service emulation is provided by [Plugin] implementations registered with
// a [PluginRegistry]. State is persisted via the [StateManager] interface.
//
// Substrate ships 39 built-in service plugins registered by
// [RegisterDefaultPlugins], covering S3, Lambda, DynamoDB, EC2, IAM, SQS,
// SNS, SES, Kinesis, Firehose, AppSync, Service Quotas, and many more. [S3Plugin]
// alone supports 23 REST+XML operations including multipart uploads,
// versioning, and lifecycle configuration. The [BettyClient]
// integrates CloudFormation deployment ([StackDeployer]), recording,
// [ValidateRecording], and [DebugSession] time-travel into a single
// high-level API for AI-generated infrastructure validation.
//
// # Plugin Development
//
// Substrate's service layer is fully extensible. Any type that satisfies the
// [Plugin] interface can be registered with a [PluginRegistry] and will
// receive matching AWS API requests. See the package-level documentation in
// doc_plugins.go for the full plugin developer guide, including state key
// conventions, testing patterns, and cross-service dispatch examples.
// See examples/custom_plugin/main.go for a minimal self-contained example.
package emulator

// Version is the Substrate release version, set at build time via ldflags.
var Version = "dev"
