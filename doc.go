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
// S3 emulation is provided by [S3Plugin] with 17 REST+XML operations including
// multipart uploads. The [BettyClient] integrates CloudFormation deployment
// ([StackDeployer]), recording, [ValidateRecording], and [DebugSession]
// time-travel into a single high-level API for AI-generated infrastructure
// validation.
package substrate
