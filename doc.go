// Package substrate provides an event-sourced AWS emulation layer for
// deterministic testing of AI-generated infrastructure code.
//
// # Overview
//
// Substrate's three core differentiators:
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
// # Quick Start
//
// Use [BettyClient] to deploy a CloudFormation template, record operations,
// and validate the results in a single workflow:
//
//	betty := substrate.NewBettyClient(registry, store, state, tc, logger)
//
//	result, err := betty.Deploy(ctx, cfnTemplate, substrate.Intent{MaxCost: 1.0})
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
// Substrate ships 38 built-in service plugins registered by
// [RegisterDefaultPlugins], covering S3, Lambda, DynamoDB, EC2, IAM, SQS,
// SNS, SES, Kinesis, Firehose, Service Quotas, and many more. [S3Plugin]
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
package substrate

// Version is the Substrate release version, set at build time via ldflags.
var Version = "dev"
